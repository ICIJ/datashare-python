import json
import re
from datetime import datetime
from pathlib import Path

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.constants import TIKA_METADATA_RESOURCENAME
from datashare_python.objects import (
    ByteRangesPagination,
    DatashareLanguage,
    Document,
    DocumentLocation,
    FilesystemDocument,
    FilesystemPagination,
    Pages,
    Task,
    TaskState,
)
from pydantic import TypeAdapter, ValidationError


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
        FilesystemDocument(
            id="some_id",
            path=path,
            index="id",
            location=DocumentLocation.ORIGINAL,
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
    fs_doc = doc.to_filesystem()
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
