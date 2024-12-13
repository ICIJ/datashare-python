# --8<-- [start:hello_world]
from icij_worker import AsyncApp
from icij_worker.app import TaskGroup

# --8<-- [start:create_app]
app = AsyncApp("some-app")
# --8<-- [end:create_app]

# --8<-- [start:create_task_group]
PYTHON_TASK_GROUP = TaskGroup(name="PYTHON")


# --8<-- [end:create_task_group]


@app.task(group=PYTHON_TASK_GROUP)
def hello_world(user: dict | None = None) -> str:
    return "Hello world"


# --8<-- [end:hello_world]


# --8<-- [start:hello_user]
@app.task(group=PYTHON_TASK_GROUP)
def hello_user(user: dict | None) -> str:
    greeting = "Hello "
    if user is None:
        user = "unknown"
    else:
        user = user["id"]
    return greeting + user


# --8<-- [end:hello_user]

# --8<-- [start:hello_user_progress]
from icij_worker.typing_ import RateProgress


@app.task(group=PYTHON_TASK_GROUP)
async def hello_user_progress(user: dict | None, progress: RateProgress) -> str:
    greeting = "Hello "
    await progress(0.5)
    if user is None:
        user = "unknown"
    else:
        user = user["id"]
    res = greeting + user
    await progress(1)
    return res


# --8<-- [end:hello_user_progress]
