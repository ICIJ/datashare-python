import json
import re
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from datashare_python.conftest import TEST_PROJECT
from datashare_python.constants import TIKA_METADATA_RESOURCENAME
from datashare_python.objects import (
    BaseModel,
    ByteRangesPagination,
    DatashareLanguage,
    Document,
    DocumentLocation,
    FilesystemPagination,
    ManifestEntry,
    Pages,
    ProcessedFile,
    Task,
    TaskArgs,
    TaskState,
)
from pydantic import TypeAdapter, ValidationError
from temporalio import activity


class MockedManifestEntry(ManifestEntry): ...


class MockedArgs(TaskArgs): ...


def test_task_ser() -> None:
    # Given
    task = Task(id="some_id", name="some_name", args=dict())

    # When
    serialized = task.model_dump()

    # Then
    assert isinstance(serialized.pop("createdAt"), datetime)
    expected = {
        "@type": "Task",
        "args": {},
        "completedAt": None,
        "error": None,
        "id": "some_id",
        "maxRetries": None,
        "name": "some_name",
        "progress": None,
        "result": None,
        "retriesLeft": None,
        "state": TaskState.CREATED,
    }
    assert serialized == expected


def test_filesystem_document_should_raise_on_absolute_path() -> None:
    # Given
    path = Path("/some/absolute/path")
    # When/Then
    expected = re.escape("FilesystemDocument path should always be relative")
    with pytest.raises(ValidationError, match=expected):
        ProcessedFile(
            id="some_id",
            path=path,
            project="id",
            location=DocumentLocation.FILESYSTEM,
            resource_name="aa",
        )


def test_document_to_filesystem_document_use_relative_path() -> None:
    # Given
    path = Path("/some/absolute/path/resource.file")
    assert path.is_absolute()
    meta = {TIKA_METADATA_RESOURCENAME: "resource.file"}
    doc = Document(
        index=TEST_PROJECT, path=path, id="some_id", language="ENGLISH", metadata=meta
    )
    # When
    fs_doc = doc.to_processed_file()
    relative_path = Path("some/absolute/path/resource.file")
    assert fs_doc.path == relative_path


def test_datashare_language() -> None:
    # Given
    language = "ENGLISH"
    type_adapter = TypeAdapter(DatashareLanguage)
    # When
    ds_language = type_adapter.validate_python(language)
    # Then
    assert isinstance(ds_language, DatashareLanguage)
    assert ds_language == language


@pytest.mark.parametrize(
    ("language", "expected_msg"),
    [("English", "expected uppercase"), ("AAAA", "Unknown")],
)
def test_invalid_datashare_language_should_raise(
    language: str, expected_msg: str
) -> None:
    # Given
    type_adapter = TypeAdapter(DatashareLanguage)

    # When/Then
    with pytest.raises(ValidationError, match=expected_msg):
        type_adapter.validate_python(language)


@pytest.mark.parametrize(
    ("pages", "expected_serialized"),
    [
        (
            Pages(
                pagination=ByteRangesPagination(byte_ranges=[(0, 1), (1, 2), (2, 3)]),
                total=3,
            ),
            {
                "pagination": {
                    "byteRanges": [[0, 1], [1, 2], [2, 3]],
                    "type": "byteRanges",
                },
                "total": 3,
            },
        ),
        (
            Pages(pagination=FilesystemPagination(), total=3),
            {"pagination": {"type": "filesystem"}, "total": 3},
        ),
    ],
)
def test_pages_serde(pages: Pages, expected_serialized: dict) -> None:
    # When
    serialized = pages.model_dump_json(by_alias=True)
    deserialized = Pages.model_validate_json(serialized)
    # Then
    assert json.loads(serialized) == expected_serialized
    assert deserialized == pages


def test_pages_validation_should_raise_for_inconsistent_byte_ranges() -> None:
    # When
    expected = "byte_ranges must match total"
    with pytest.raises(ValidationError, match=expected):
        Pages(
            pagination=ByteRangesPagination(byte_ranges=[(0, 1), (1, 2), (2, 3)]),
            total=2,
        )


@pytest.mark.parametrize("in_activity", [True, False])
def test_manifest_entry_complete_task_id(
    *, in_activity: bool, monkeypatch: MonkeyPatch
) -> None:
    # Given
    args = MockedArgs()
    mocked_info = MagicMock()
    type(mocked_info).workflow_id = PropertyMock(return_value="some_value")
    if in_activity:
        monkeypatch.setattr(activity, "in_activity", lambda: True)
        monkeypatch.setattr(activity, "info", lambda: mocked_info)
    # When
    manifest_entry = MockedManifestEntry.complete(args)
    # Then
    if in_activity:
        assert manifest_entry.task_id is not None
    else:
        assert manifest_entry.task_id is None


@pytest.mark.parametrize("in_activity", [True, False])
def test_manifest_entry_partial_task_id(
    *, in_activity: bool, monkeypatch: MonkeyPatch
) -> None:
    # Given
    args = MockedArgs()
    mocked_info = MagicMock()
    type(mocked_info).workflow_id = PropertyMock(return_value="some_value")
    if in_activity:
        monkeypatch.setattr(activity, "in_activity", lambda: True)
        monkeypatch.setattr(activity, "info", lambda: mocked_info)
    # When
    manifest_entry = MockedManifestEntry.partial(args)
    # Then
    if in_activity:
        assert manifest_entry.task_id is not None
    else:
        assert manifest_entry.task_id is None


def test_base_model_hash_is_constant() -> None:
    # Given
    class MockConfig(BaseModel):
        some_key: str = "some_value"

    cfg = MockConfig()

    # When
    hashed = hash(cfg)
    assert hashed == 1821116217857887821
