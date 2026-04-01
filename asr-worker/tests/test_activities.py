import json
import math
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from aiostream import stream
from asr_worker.activities import (
    search_audios,
    write_audio_search_results,
    write_transcription,
)
from asr_worker.models import Timestamp, Transcript, Transcription
from caul.model_handlers.objects import ASRModelHandlerResult
from constants import SUPPORTED_CONTENT_TYPES
from datashare_python.conftest import TEST_PROJECT
from icij_common.es import HITS, ESClient, ids_query, match_all


def test_write_transcription(tmpdir: Path) -> None:
    # Given
    asr_result = ASRModelHandlerResult(
        transcription=[(0.0, 1.0, "text")], score=math.log(0.5)
    )
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
    populate_es: ESClient,  # noqa: ARG001
    test_es_client: ESClient,
    query: dict,
    expected_docs: list[str],
) -> None:
    # Given
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

    async def results() -> AsyncGenerator[dict, None]:
        res = [{HITS: {HITS: ["doc-0", "doc-1", "doc-2"]}}]
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

    async with batches_and_expected_content as streamed:
        async for p, expected_content in streamed:
            assert p.exists()
            assert p.read_text() == expected_content
