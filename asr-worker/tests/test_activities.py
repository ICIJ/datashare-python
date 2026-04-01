import json
import math
from collections.abc import AsyncGenerator, Iterable
from pathlib import Path
from typing import Self

import pytest
from aiostream import stream
from asr_worker.activities import (
    preprocess,
    search_audios,
    write_audio_search_results,
    write_transcription,
)
from asr_worker.constants import SUPPORTED_CONTENT_TYPES
from asr_worker.models import Timestamp, Transcript, Transcription
from asr_worker.utils import read_jsonl
from caul.objects import ASRResult, InputMetadata, PreprocessedInput
from caul.tasks import Preprocessor
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import Document
from icij_common.es import ESClient, ids_query, match_all
from icij_common.registrable import RegistrableConfig

PREPROCESSED_INPUT_0 = PreprocessedInput(metadata=InputMetadata(duration_s=0.0))
PREPROCESSED_INPUT_1 = PreprocessedInput(metadata=InputMetadata(duration_s=1.0))
PREPROCESSED_INPUT_2 = PreprocessedInput(metadata=InputMetadata(duration_s=2.0))


class MockProcessor(Preprocessor):
    @classmethod
    def _from_config(cls, config: RegistrableConfig, **kwargs) -> Self:  # noqa: ARG003
        return cls(**kwargs)

    def __init__(self, batches: list[list[PreprocessedInput]]) -> None:
        self._batches = batches

    def process(
        self,
        audios: Iterable[Path],  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> Iterable[list[PreprocessedInput]]:
        yield from self._batches


def test_preprocess(tmpdir: Path) -> None:
    # Given
    output_dir = Path(tmpdir)
    batches = [[PREPROCESSED_INPUT_0, PREPROCESSED_INPUT_1], [PREPROCESSED_INPUT_2]]
    preprocessor = MockProcessor(batches)
    audios = []
    # When
    batch_files = list(preprocess(preprocessor, audios, output_dir=output_dir))
    # Then
    assert len(batch_files) == 2
    written_batches = [
        [PreprocessedInput.model_validate(d) for d in read_jsonl(f)]
        for f in batch_files
    ]
    assert written_batches == batches


def test_write_transcription(tmpdir: Path) -> None:
    # Given
    asr_result = ASRResult(transcription=[(0.0, 1.0, "text")], score=math.log(0.5))
    transcribed_filename = "0011someid"
    artifacts_root = Path(tmpdir)
    project = TEST_PROJECT
    # When
    write_transcription(
        asr_result, transcribed_filename, artifacts_root=artifacts_root, project=project
    )
    # Then
    expected_artifact_dir = artifacts_root / project / "00" / "11" / "0011someid"
    assert expected_artifact_dir.exists()
    metadata_path = expected_artifact_dir / "metadata.json"
    assert metadata_path.exists()
    metadata = json.loads(metadata_path.read_text())
    assert metadata["transcription"] == "transcription.json"
    transcription_path = expected_artifact_dir / metadata["transcription"]
    assert transcription_path.exists()
    transcription = Transcription.model_validate_json(transcription_path.read_text())
    expected_transcription = Transcription(
        transcripts=[
            Transcript(text="text", timestamp=Timestamp(start_s=0.0, end_s=1.0))
        ],
        confidence=0.5,
    )
    assert transcription == expected_transcription


@pytest.mark.parametrize(
    ("query", "expected_docs"),
    [
        # Supports empty query
        ({}, ["doc-0", "doc-2"]),
        # Return all audio/video docs
        (match_all(), ["doc-0", "doc-2"]),
        (ids_query(["doc-0"]), ["doc-0"]),
        # Should filter non supported content type
        (ids_query(["doc-1"]), []),
    ],
)
async def test_search_audios(
    populate_es_with_audio: list[Document],
    test_es_client: ESClient,
    query: dict,
    expected_docs: list[str],
) -> None:
    # Given
    assert len(populate_es_with_audio) == 4
    client = test_es_client
    # When
    results = search_audios(
        es_client=client,
        project=TEST_PROJECT,
        query=query,
        supported_content_types=SUPPORTED_CONTENT_TYPES,
    )
    # Then
    docs_ids = [i async for i in results]
    assert docs_ids == expected_docs


async def test_write_audio_search_results(tmpdir: Path) -> None:
    # Given
    root = Path(tmpdir)
    batch_size = 2

    async def results() -> AsyncGenerator[str, None]:
        res = ["doc-0", "doc-1", "doc-2"]
        for r in res:
            yield r

    # When
    batches = write_audio_search_results(results(), root=root, batch_size=batch_size)

    # Then
    async def expected_content() -> AsyncGenerator[str, None]:
        contents = ["doc-0\ndoc-1\n", "doc-2\n"]
        for e in contents:
            yield e

    batches_and_expected_content = stream.zip(batches, expected_content())

    async with batches_and_expected_content.stream() as streamed:
        async for p, expected_content in streamed:
            assert p.exists()
            assert p.read_text() == expected_content
