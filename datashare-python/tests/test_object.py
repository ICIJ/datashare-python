from datetime import datetime

from datashare_python.objects import Task, TaskState


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
