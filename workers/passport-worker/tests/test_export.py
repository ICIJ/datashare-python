from collections.abc import AsyncIterable, Iterable
from pathlib import Path

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import Document, WorkerPaths
from datashare_python.utils import write_artifact
from icij_common.es import ESClient
from passport_service.objects import Passport
from passport_worker.config import PassportWorkerConfig
from passport_worker.export import export_as_csv, export_as_jsonl
from passport_worker.objects import (
    PagePassports,
    PassportArtifact,
    PassportDetectionArgs,
    PassportDetectionConfig,
    PassportInferenceConfig,
    PassportManifestEntry,
    Passports,
    YOLOPassportDetectorConfig,
)


@pytest.fixture
def artifact_dir_with_passports(
    populate_es: list[Document],
    test_worker_config: PassportWorkerConfig,
    test_model_path: Path,
) -> tuple[WorkerPaths, list[Document]]:
    passport_detector_config = YOLOPassportDetectorConfig(
        model_path=test_model_path, onnx_provider="CPUExecutionProvider"
    )
    args = PassportDetectionArgs(
        project=TEST_PROJECT,
        docs=None,
        config=PassportDetectionConfig(
            inference=PassportInferenceConfig(
                passport_detector=passport_detector_config
            )
        ),
    )
    manifest_entry = PassportManifestEntry.complete(args)
    paths = test_worker_config.paths
    passports = [Passport(class_id="passport", confidence=1.0, box=(0, 0, 100, 100))]
    for d in populate_es:
        if "not" in d.path.name:
            continue
        passport_pages = []
        for i in range(d.metadata.get("tika_metadata_xmptpg_npages", 1)):
            passport_pages.append(PagePassports(page_number=i + 1, passports=passports))
        passport_artifact = PassportArtifact(
            project=d.project,
            doc_id=d.doc_id,
            manifest_entry=manifest_entry,
            artifact=Passports(pages=passport_pages).model_dump_json().encode(),
        )
        write_artifact(paths.artifacts, passport_artifact)
    return paths, populate_es


async def _async_iter[R](it: Iterable[R]) -> AsyncIterable[R]:
    for r in it:
        yield r


async def test_export_as_csv(
    artifact_dir_with_passports: tuple[WorkerPaths, list[Document]],
    test_es_client: ESClient,
) -> None:
    # Given
    paths, docs = artifact_dir_with_passports
    docs = (d async for d in _async_iter(docs))
    # When
    csv = [csv_l async for csv_l in export_as_csv(docs, "https://my-datashare", paths)]

    # Then
    assert csv[0] == "page,doc_id,project,confidence,root_id,url"
    expected_l = (
        "1,doc-2,test-project,1.0,doc-2,"
        "https://my-datashare/#/ds/test-project/doc-2/doc-2?tab=viewer"
    )
    assert csv[1] == expected_l
    assert len(csv) == 1 + 5  # header + 5 docs with a single passport page in them


async def test_export_as_jsonl(
    artifact_dir_with_passports: tuple[WorkerPaths, list[Document]],
) -> None:
    # Given
    paths, docs = artifact_dir_with_passports
    docs = (d async for d in _async_iter(docs))
    # When
    jsonl = [csv_l async for csv_l in export_as_jsonl(docs, paths)]

    # Then
    assert len(jsonl) == 5
    assert isinstance(Passports.model_validate_json(jsonl[0]), Passports)
