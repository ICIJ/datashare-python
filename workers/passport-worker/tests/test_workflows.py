import json
import uuid
from pathlib import Path

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.objects import ProcessedFile
from datashare_python.utils import safe_dir
from passport_worker.config import PassportWorkerConfig
from passport_worker.objects import (
    PassportDetectionArgs,
    PassportDetectionConfig,
    PassportDetectionResponse,
    PassportInferenceConfig,
    PassportManifestEntry,
    Passports,
    ProcessingReport,
    YOLOPassportDetectorConfig,
)
from passport_worker.workflows import PassportDetectionWorkflow, TaskQueue
from temporalio.client import Client as TemporalClient
from temporalio.worker import Worker

from tests import DOCS_PATH


@pytest.mark.e2e
async def test_passport_detection_workflow(
    workflows_worker: Worker,  # noqa: ARG001
    io_worker: Worker,  # noqa: ARG001
    preprocessing_worker: Worker,  # noqa: ARG001
    inference_worker: Worker,  # noqa: ARG001
    test_worker_config: PassportWorkerConfig,
    test_temporal_client: TemporalClient,
    test_model_path: Path,
    e2e_docs: list[ProcessedFile],
) -> None:
    # Given
    temporal_client = test_temporal_client
    worker_paths = test_worker_config.paths
    docs = [d.id for d in e2e_docs]
    passport_detector_path = YOLOPassportDetectorConfig(model_path=test_model_path)
    args = PassportDetectionArgs(
        project=TEST_PROJECT,
        docs=docs,
        config=PassportDetectionConfig(
            inference=PassportInferenceConfig(passport_detector=passport_detector_path)
        ),
    )
    wf_id = f"detect-passports-{uuid.uuid4()}"

    # When
    response = await temporal_client.execute_workflow(
        PassportDetectionWorkflow,
        args,
        id=wf_id,
        task_queue=TaskQueue.WORKFLOWS,
    )

    # Then
    expected_response = PassportDetectionResponse(
        processed=ProcessingReport(n_docs=6, n_pages=6),
        successes=ProcessingReport(n_docs=6, n_pages=6),
    )
    assert response.model_dump() == expected_response.model_dump()
    expected = []
    for f in DOCS_PATH.iterdir():
        if not f.is_file():
            continue
        has_manifest = f.suffix != ".eml"
        has_passport = "not_a" not in f.name
        expected.append((f"e2e-doc-{len(expected)}", has_manifest, has_passport))
    expected_manifest_entry = PassportManifestEntry.complete(args)
    for doc_id, has_manifest, has_passport in expected:
        artifacts_path = (
            worker_paths.artifacts / TEST_PROJECT / safe_dir(doc_id) / doc_id
        )
        passports_path = artifacts_path / "passports.json"
        manifest_path = artifacts_path / "manifest.json"
        if not has_manifest:
            assert not manifest_path.exists()
            assert not passports_path.exists()
            continue
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        manifest_entry = PassportManifestEntry.model_validate(manifest["passports"])
        assert manifest_entry == expected_manifest_entry
        assert passports_path.exists()
        passports = Passports.model_validate_json(passports_path.read_text())
        if not has_passport:
            assert not passports.pages
        else:
            assert len(passports.pages) == 1
            passports = passports.pages[0]
            assert passports.page_number == 1
            assert len(passports.passports) == 2
            with_mrz = [
                passport_page
                for passport_page in passports.passports
                if passport_page.mrz is not None
            ]
            assert len(with_mrz) == 1
            with_mrz = with_mrz[0]
            assert with_mrz.mrz is not None
            assert with_mrz.mrz.metadata["names"] == "JANE"
