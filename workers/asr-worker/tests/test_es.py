from functools import partial
from pathlib import Path

import pytest
from asr_worker.config import ASRWorkerConfig
from asr_worker.es import index_transcriptions_act, search_audios_act
from asr_worker.objects import (
    ASRArgs,
    TranscriptionArtifact,
    TranscriptionManifestEntry,
)
from asr_worker.postprocessing import write_transcription
from caul_core import ASRResult
from datashare_python.objects import PROCESSED_FILE_TA, Document
from datashare_python.utils import read_jsonl_as
from icij_common.es import HITS, ESClient, ids_query, match_all

from .conftest import DS_ENGLISH, TEST_PROJECT


@pytest.mark.parametrize(
    ("query", "expected_doc_ids"),
    [
        # Supports empty query
        ({}, [["doc-0", "doc-2"]]),
        # Return all audio/video docs
        (match_all(), [["doc-0", "doc-2"]]),
        (ids_query(["doc-0"]), [["doc-0"]]),
        # Should filter non supported content type
        (ids_query(["doc-1"]), []),
    ],
)
async def test_search_audio_paths_act(  # noqa: PLR0917
    with_audio_docs: list[Document],  # noqa: F821
    test_worker_config: ASRWorkerConfig,
    test_es_client: ESClient,
    query: dict,
    expected_doc_ids: list[list[str]],
    tmpdir: Path,
) -> None:
    # Given
    paths = test_worker_config.paths
    tmpdir = Path(tmpdir)
    batch_size = len(with_audio_docs)
    client = test_es_client
    # When
    batch_paths = [
        batch
        async for batch in search_audios_act(
            TEST_PROJECT, paths, client, query, batch_size=batch_size, output_dir=tmpdir
        )
    ]
    # Then
    doc_ids = []
    for b in batch_paths:
        # assert b.read_text() == EXPECTED
        doc_ids.append([d.doc_id for d in read_jsonl_as(b, PROCESSED_FILE_TA)])
    assert doc_ids == expected_doc_ids


@pytest.fixture
def with_transcribed_docs(
    with_audio_docs: list[Document], test_worker_config: ASRWorkerConfig
) -> list[tuple[Document, str]]:
    artifacts_root = test_worker_config.paths.artifacts
    transcriptions = []
    args = ASRArgs(project=TEST_PROJECT, docs=[], batch_size=2, language=DS_ENGLISH)
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
    routes = dict(d.route for d in docs)
    transcriptions = list(transcriptions)
    # When
    n_docs = await index_transcriptions_act(
        routes,
        project=TEST_PROJECT,
        es_client=test_es_client,
        artifact_root=test_worker_config.paths.artifacts,
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
