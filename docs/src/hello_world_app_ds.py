# --8<-- [start:app]
# --8<-- [start:hello_world]
from icij_worker import AsyncApp
from icij_worker.app import TaskGroup
from icij_worker.typing_ import RateProgress

# --8<-- [start:create_app]
app = AsyncApp("some-app")

PYTHON_TASK_GROUP = TaskGroup(name="PYTHON")


# --8<-- [end:create_app]
@app.task(name="hello_world", group=PYTHON_TASK_GROUP)
def hello_world() -> str:
    return "Hello world"


# --8<-- [end:hello_world]
# --8<-- [start:hello_user]
@app.task(name="hello_user", group=PYTHON_TASK_GROUP)
def hello_user(user: str | None) -> str:
    greeting = "Hello "
    if user is None:
        user = "unknown"
    return greeting + user


# --8<-- [end:hello_user]
# --8<-- [start:hello_user_progress]
@app.task(name="hello_user_progress", group=PYTHON_TASK_GROUP)
async def hello_user_progress(user: str | None, progress: RateProgress) -> str:
    greeting = "Hello "
    await progress(0.5)
    if user is None:
        user = "unknown"
    res = greeting + user
    await progress(1)
    return res


# --8<-- [end:hello_user_progress]
# --8<-- [start:app]
