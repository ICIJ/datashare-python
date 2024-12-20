# --8<-- [start:app]
from icij_worker import AsyncApp
from icij_worker.app import TaskGroup

app = AsyncApp("some-app")

PYTHON_TASK_GROUP = TaskGroup(name="PYTHON")


# --8<-- [end:hello_world]
@app.task(name="hello_user", group=PYTHON_TASK_GROUP)
# --8<-- [start:hello_user_fn]
def hello_user(user: dict | None) -> str:
    greeting = "Hello "
    if user is None:
        user = "unknown"
    else:
        user = user["id"]
    return greeting + user


# --8<-- [end:hello_user_fn]
# --8<-- [end:app]
