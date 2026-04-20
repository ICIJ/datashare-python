import json
from collections.abc import AsyncGenerator, Iterable
from itertools import cycle
from pathlib import Path
from typing import Self

import pytest
from aiostream import stream
from asr_worker.activities import (
    infer_act,
    postprocess_act,
    preprocess_act,
    search_audio_paths_act,
    write_audio_batches,
)
from asr_worker.config import ASRWorkerConfig
from asr_worker.objects import DocId, Transcription
from asr_worker.utils import read_jsonl
from caul.objects import ASRResult, InputMetadata, PreprocessedInput, PreprocessorOutput
from caul.tasks import InferenceRunner, Postprocessor, Preprocessor
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import DocumentLocation, FilesystemDocument
from icij_common.es import ESClient, ids_query, match_all
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

FS_DOCUMENT_0 = FilesystemDocument(
    id="doc-0",
    path=Path(TEST_PROJECT, "symlinks", "do", "c-", "doc-0", "raw.wav"),
    index=TEST_PROJECT,
    location=DocumentLocation.WORKDIR,
    resource_name="doc-0.wav",
)
DS_DOCUMENT_2 = FilesystemDocument(
    id="doc-2",
    path=Path("doc-2.mp3"),
    index=TEST_PROJECT,
    location=DocumentLocation.ORIGINAL,
    resource_name="doc-2.mp3",
)


class MockPreprocessor(Preprocessor):
    def __init__(self, batch_size: int) -> None:
        self._batch_size = batch_size

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
        ({}, [[FS_DOCUMENT_0, DS_DOCUMENT_2]]),
        # Return all audio/video docs
        (match_all(), [[FS_DOCUMENT_0, DS_DOCUMENT_2]]),
        (ids_query(["doc-0"]), [[FS_DOCUMENT_0]]),
        # Should filter non supported content type
        (ids_query(["doc-1"]), []),
    ],
)
async def test_search_audio_paths_act(
    with_audio_docs: list[FilesystemDocument],
    test_es_client: ESClient,
    query: dict,
    expected_batches: list[tuple[DocId, Path]],
    test_worker_config: ASRWorkerConfig,
    tmpdir: Path,
) -> None:
    # Given
    tmpdir = Path(tmpdir)
    worker_config = test_worker_config
    batch_size = len(with_audio_docs)
    client = test_es_client
    # When
    batch_paths = [
        batch
        async for batch in search_audio_paths_act(
            es_client=client,
            project=TEST_PROJECT,
            query=query,
            batch_size=batch_size,
            output_dir=tmpdir,
            config=worker_config,
        )
    ]
    # Then
    results = []
    for b in batch_paths:
        results.append(
            [FilesystemDocument.model_validate(fs_doc) for fs_doc in read_jsonl(b)]
        )
    assert results == expected_batches


def test_preprocess_act(test_worker_config: ASRWorkerConfig, tmpdir: Path) -> None:
    # Given
    output_dir = Path(tmpdir)
    n_audios = 3
    batch_size = n_audios - 1
    audio_batch = tmpdir / "audio_batch.txt"
    batch = [
        FilesystemDocument(
            id=f"doc-{i}",
            path=Path(str(i)),
            location=DocumentLocation.ARTIFACTS,
            index=TEST_PROJECT,
            resource_name=f"doc-{i}.wav",
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


def test_infer_act(tmpdir: Path) -> None:
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
        for p in asr_result_paths
    ]
    assert asr_results == INFERENCE_RESULTS


def test_postprocess_act(tmpdir: Path) -> None:
    # Given
    postprocessor = MockPostprocessor()
    project = TEST_PROJECT
    artifacts_root = Path(tmpdir)
    doc_ids = [f"{str(i) * 4}-doc-{i}" for i in range(3)]
    # When
    postprocess_act(
        postprocessor,
        INFERENCE_RESULTS,
        doc_ids=doc_ids,
        artifacts_root=artifacts_root,
        project=project,
    )
    # Then
    expected_artifact_dirs = [
        artifacts_root / project / "00" / "00" / "0000-doc-0",
        artifacts_root / project / "11" / "11" / "1111-doc-1",
        artifacts_root / project / "22" / "22" / "2222-doc-2",
    ]
    for res, d in zip(INFERENCE_RESULTS, expected_artifact_dirs, strict=True):
        assert d.exists()
        metadata_path = d / "metadata.json"
        assert metadata_path.exists()
        metadata = json.loads(metadata_path.read_text())
        assert metadata["transcription"] == "transcription.json"
        transcription_path = d / metadata["transcription"]
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

    async def results() -> AsyncGenerator[FilesystemDocument, None]:
        res = ["doc-0", "doc-1", "doc-2"]
        for r in res:
            fs_doc = FilesystemDocument(
                id=r,
                path=Path(f"{r}.wav"),
                index=TEST_PROJECT,
                location=DocumentLocation.WORKDIR,
                resource_name=f"{r}.wav",
            )
            yield fs_doc

    # When
    results = write_audio_batches(results(), root=root, batch_size=batch_size)

    # Then
    async def expected_content() -> AsyncGenerator[str, None]:
        contents = [
            [
                '{"id":"doc-0","path":"doc-0.wav","index":"test-project","location":"workdir","resource_name":"doc-0.wav"}',
                '{"id":"doc-1","path":"doc-1.wav","index":"test-project","location":"workdir","resource_name":"doc-1.wav"}',
            ],
            [
                '{"id":"doc-2","path":"doc-2.wav","index":"test-project","location":"workdir","resource_name":"doc-2.wav"}'
            ],
        ]
        for line in contents:
            yield "\n".join(line) + "\n"

    batches_and_expected_content = stream.zip(results, expected_content())

    async with batches_and_expected_content.stream() as streamed:
        async for p, expected_content in streamed:
            assert p.exists()
            assert p.read_text() == expected_content
