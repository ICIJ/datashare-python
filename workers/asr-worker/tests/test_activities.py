import json
from collections.abc import AsyncGenerator, Iterable
from functools import partial
from itertools import cycle
from pathlib import Path
from typing import Self

import pytest
from aiostream import stream
from asr_worker.activities import (
    index_transcriptions_act,
    infer_act,
    postprocess_act,
    preprocess_act,
    search_audios_act,
    write_audio_batches,
    write_transcription,
)
from asr_worker.config import ASRWorkerConfig
from asr_worker.objects import (
    ASRArgs,
    Transcription,
    TranscriptionArtifact,
    TranscriptionManifestEntry,
)
from caul_core import (
    ASRResult,
    InferenceRunner,
    InputMetadata,
    Postprocessor,
    PreprocessedInput,
    Preprocessor,
    PreprocessorOutput,
)
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import (
    DatashareLanguage,
    Document,
    FilesystemDocument,
)
from datashare_python.utils import read_jsonl
from icij_common.es import HITS, ESClient, ids_query, match_all
from icij_common.iter_utils import batches
from icij_common.registrable import RegistrableConfig

PREPROCESSED_INPUT_0 = PreprocessedInput(
    metadata=InputMetadata(
        input_ordering=0,
        duration_s=0.0,
        preprocessed_file_path=Path("preprocessed_0.wav"),
    )
)
PREPROCESSED_INPUT_1 = PreprocessedInput(
    metadata=InputMetadata(
        input_ordering=1,
        duration_s=1.0,
        preprocessed_file_path=Path("preprocessed_1.wav"),
    )
)
PREPROCESSED_INPUT_2 = PreprocessedInput(
    metadata=InputMetadata(
        input_ordering=2,
        duration_s=2.0,
        preprocessed_file_path=Path("preprocessed_2.wav"),
    )
)

INFERENCE_RESULTS = [
    ASRResult(
        input_ordering=0,
        transcription=[(0.0, 0.0, "preprocessed_0")],
        score=1.0,
    ),
    ASRResult(
        input_ordering=1,
        transcription=[(0.0, 1.0, "preprocessed_1")],
        score=1.0,
    ),
    ASRResult(
        input_ordering=2,
        transcription=[(0.0, 2.0, "preprocessed_2")],
        score=1.0,
    ),
]

DOC_0 = Document(
    id="doc-0",
    path=Path(TEST_PROJECT, "symlinks", "do", "c-", "doc-0", "doc-0.wav"),
    index=TEST_PROJECT,
    language=DatashareLanguage("ENGLISH"),
)
DOC_2 = Document(
    id="doc-2",
    path=Path("doc-2.mp3"),
    index=TEST_PROJECT,
    language=DatashareLanguage("ENGLISH"),
)


class MockPreprocessor(Preprocessor):
    def __init__(self, batch_size: int) -> None:
        self._batch_size = batch_size

    @classmethod
    def cache_models(cls, cache_dir: Path | None = None) -> None: ...

    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls(**kwargs)

    def process(
        self,
        audios: Iterable[Path],  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[list[PreprocessedInput]]:
        outputs = cycle(
            [PREPROCESSED_INPUT_0, PREPROCESSED_INPUT_1, PREPROCESSED_INPUT_2]
        )
        outputs = [next(outputs) for _ in audios]
        for b in batches(outputs, self._batch_size):
            yield list(b)


class MockInferenceRunner(InferenceRunner):
    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls()

    @classmethod
    def cache_models(cls, cache_dir: Path | None = None) -> None: ...

    def process(
        self,
        inputs: Iterable[list[PreprocessorOutput]],
        *args,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ASRResult]:
        i = 0
        for batch in inputs:
            for preprocessed in batch:
                transcription = (
                    preprocessed.metadata.preprocessed_file_path.name.replace(
                        ".wav", ""
                    )
                )
                transcription = [(0.0, float(i), transcription)]
                yield ASRResult(
                    input_ordering=i, transcription=transcription, score=1.0
                )
                i += 1


class MockPostprocessor(Postprocessor):
    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls()

    def process(
        self,
        inputs: Iterable[ASRResult],
        *args,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[ASRResult]:
        yield from inputs


@pytest.mark.parametrize(
    ("query", "expected_batches"),
    [
        # Supports empty query
        ({}, [["doc_0", "doc_2"]]),
        # Return all audio/video docs
        (match_all(), [["doc_0", "doc_2"]]),
        (ids_query(["doc-0"]), [["doc_0"]]),
        # Should filter non supported content type
        (ids_query(["doc-1"]), []),
    ],
)
async def test_search_audio_paths_act(
    with_audio_docs: list[FilesystemDocument],
    test_es_client: ESClient,
    query: dict,
    expected_batches: list[list[str]],
    request,  # noqa: ANN001
    tmpdir: Path,
) -> None:
    # Given
    tmpdir = Path(tmpdir)
    batch_size = len(with_audio_docs)
    client = test_es_client
    # When
    batch_paths = [
        batch
        async for batch in search_audios_act(
            es_client=client,
            project=TEST_PROJECT,
            query=query,
            batch_size=batch_size,
            output_dir=tmpdir,
        )
    ]
    # Then
    results = []
    for b in batch_paths:
        results.append([Document.model_validate(fs_doc).id for fs_doc in read_jsonl(b)])
    expected_batches = [
        [request.getfixturevalue(d).id for d in batch] for batch in expected_batches
    ]
    assert results == expected_batches


def test_preprocess_act(test_worker_config: ASRWorkerConfig, tmpdir: Path) -> None:
    # Given
    output_dir = Path(tmpdir)
    n_audios = 3
    batch_size = n_audios - 1
    audio_batch = tmpdir / "audio_batch.txt"
    batch = [
        Document(
            id=f"doc-{i}",
            language=DatashareLanguage("ENGLISH"),
            root_document=f"root-{i}",
            path=Path(str(i)),
            index=TEST_PROJECT,
            metadata={"tika_metadata_resourcename": f"doc-{i}.wav"},
        )
        for i in range(n_audios)
    ]
    with audio_batch.open("w") as f:
        for fs_doc in batch:
            f.write(fs_doc.model_dump_json() + "\n")
    preprocessor = MockPreprocessor(batch_size=batch_size)

    # When
    batch_files = preprocess_act(
        preprocessor,
        audio_batch=audio_batch,
        worker_config=test_worker_config,
        output_dir=output_dir,
    )

    # Then
    assert len(batch_files) == 2
    expected_batches = [
        [PREPROCESSED_INPUT_0, PREPROCESSED_INPUT_1],
        [PREPROCESSED_INPUT_2],
    ]
    written_batches = [
        [PreprocessedInput.model_validate(d) for d in read_jsonl(output_dir / f)]
        for f in batch_files
    ]
    assert written_batches == expected_batches


async def test_infer_act(tmpdir: Path) -> None:
    # Given
    inference_runner = MockInferenceRunner()
    workdir = Path(tmpdir) / "workdir"
    workdir.mkdir()
    output_dir = Path(tmpdir)
    preprocessed_inputs = [
        PREPROCESSED_INPUT_0,
        PREPROCESSED_INPUT_1,
        PREPROCESSED_INPUT_2,
    ]
    paths = []
    for p_i, p in enumerate(preprocessed_inputs):
        input_path = workdir / f"{p_i}.json"
        input_path.write_text(p.model_dump_json())
        paths.append(input_path)
    # When
    asr_result_paths = infer_act(
        inference_runner, preprocessed_inputs=paths, output_dir=output_dir
    )
    # Then
    asr_results = [
        ASRResult.model_validate_json((output_dir / p).read_text())
        async for p in asr_result_paths
    ]
    assert asr_results == INFERENCE_RESULTS


def test_postprocess_act(tmpdir: Path) -> None:
    # Given
    args = ASRArgs(project=TEST_PROJECT, docs=[], batch_size=2)
    postprocessor = MockPostprocessor()
    project = TEST_PROJECT
    artifacts_root = Path(tmpdir)
    docs = [
        Document(
            id=f"{str(i) * 4}-doc-{i}",
            language=DatashareLanguage("ENGLISH"),
            root_document=f"root-{i}",
            path=Path(str(i)),
            index=TEST_PROJECT,
            metadata={"tika_metadata_resourcename": f"doc-{i}.wav"},
        )
        for i in range(3)
    ]
    # When
    routes = postprocess_act(
        INFERENCE_RESULTS,
        docs,
        postprocessor,
        args,
        artifacts_root=artifacts_root,
    )
    # Then
    assert routes == [
        ("root-0", "0000-doc-0"),
        ("root-1", "1111-doc-1"),
        ("root-2", "2222-doc-2"),
    ]
    expected_artifact_dirs = [
        artifacts_root / project / "00" / "00" / "0000-doc-0",
        artifacts_root / project / "11" / "11" / "1111-doc-1",
        artifacts_root / project / "22" / "22" / "2222-doc-2",
    ]
    for res, d in zip(INFERENCE_RESULTS, expected_artifact_dirs, strict=True):
        assert d.exists()
        manifest_path = d / "manifest.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        assert "transcription" in manifest
        manifest_entry = TranscriptionManifestEntry.model_validate(
            manifest["transcription"]
        )
        assert manifest_entry.confidence == 1
        assert manifest_entry.input
        transcription_path = d / "transcription.json"
        assert transcription_path.exists()
        transcription = Transcription.model_validate_json(
            transcription_path.read_text()
        )
        expected_transcription = Transcription.from_asr_handler_result(res)
        assert transcription == expected_transcription


async def test_write_audio_search_results(tmpdir: Path) -> None:
    # Given
    root = Path(tmpdir)
    batch_size = 2

    async def results() -> AsyncGenerator[Document, None]:
        res = ["doc-0", "doc-1", "doc-2"]
        for r in res:
            doc = Document(
                id=r,
                path=Path(f"{r}.wav"),
                index=TEST_PROJECT,
                language=DatashareLanguage("ENGLISH"),
                metadata={"tika_metadata_resourcename": f"{r}.wav"},
            )
            yield doc

    # When
    results = write_audio_batches(results(), root=root, batch_size=batch_size)

    # Then
    async def expected_content() -> AsyncGenerator[str, None]:
        contents = [
            [
                '{"id":"doc-0","language":"ENGLISH","index":"test-project","path":"doc-0.wav","metadata":{"tika_metadata_resourcename":"doc-0.wav"}}',
                '{"id":"doc-1","language":"ENGLISH","index":"test-project","path":"doc-1.wav","metadata":{"tika_metadata_resourcename":"doc-1.wav"}}',
            ],
            [
                '{"id":"doc-2","language":"ENGLISH","index":"test-project","path":"doc-2.wav","metadata":{"tika_metadata_resourcename":"doc-2.wav"}}'
            ],
        ]
        for line in contents:
            yield "\n".join(line) + "\n"

    batches_and_expected_content = stream.zip(results, expected_content())

    async with batches_and_expected_content.stream() as streamed:
        async for p, expected_content in streamed:
            assert p.exists()
            assert p.read_text() == expected_content


@pytest.fixture
def with_transcribed_docs(
    with_audio_docs: list[Document], test_worker_config: ASRWorkerConfig
) -> list[tuple[Document, str]]:
    artifacts_root = test_worker_config.artifacts_root
    transcriptions = []
    args = ASRArgs(project=TEST_PROJECT, docs=[], batch_size=2)
    manifest_entry = TranscriptionManifestEntry.complete(args, confidence=1.0)
    for doc_i, doc in enumerate(with_audio_docs):
        artifact_factory = partial(
            TranscriptionArtifact,
            doc_id=doc.id,
            project=TEST_PROJECT,
            manifest_entry=manifest_entry,
        )
        transcription = f"transcription_{doc_i}"
        transcriptions.append(transcription)
        asr_result = ASRResult(transcription=[(0, 1, transcription)])
        write_transcription(asr_result, artifact_factory, artifacts_root)
    return list(zip(with_audio_docs, transcriptions, strict=True))


async def test_index_transcriptions_act(
    with_transcribed_docs: list[tuple[Document, str]],
    test_es_client: ESClient,
    test_worker_config: ASRWorkerConfig,
) -> None:
    # Given
    target_bulk_char_size = 1  # let's index each doc separately
    docs, transcriptions = zip(*with_transcribed_docs, strict=True)
    docs = list(docs)
    routes = [(d.root_document, d.id) for d in docs]
    transcriptions = list(transcriptions)
    # When
    n_docs = await index_transcriptions_act(
        routes,
        project=TEST_PROJECT,
        es_client=test_es_client,
        artifact_root=test_worker_config.artifacts_root,
        target_bulk_char_size=target_bulk_char_size,
    )
    # Then
    assert n_docs == len(docs)
    contents = []
    docs_ids = [d.id for d in docs]
    body = {"query": ids_query(docs_ids)}
    async for res in test_es_client.poll_search_pages(index=TEST_PROJECT, body=body):
        contents += [Document.from_es(d).content for d in res[HITS][HITS]]
    assert contents == transcriptions
