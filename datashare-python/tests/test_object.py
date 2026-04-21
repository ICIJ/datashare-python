import re
from datetime import datetime
from pathlib import Path

import pytest
from datashare_python.conftest import TEST_PROJECT
from datashare_python.constants import TIKA_METADATA_RESOURCENAME
from datashare_python.objects import (
    Document,
    DocumentLocation,
    FilesystemDocument,
    Task,
    TaskState,
)
from pydantic import ValidationError


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
