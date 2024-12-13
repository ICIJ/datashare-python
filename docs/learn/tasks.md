# Creating asynchronous tasks

## Turning functions into async tasks using [`icij-worker`](https://github.com/ICIJ/icij-python/tree/main/icij-worker)

Before implementing task for Datashare, let's learn how to transform any Python function into a [task](concepts-basic.md#tasks) which can be run [asynchronously](concepts-basic.md#asynchronous) by a [worker](concepts-basic.md#workers) using the [icij-worker](https://github.com/ICIJ/icij-python/tree/main/icij-worker) lib.

Given the following function:
```python
def hello_world() -> str:
    return "Hello world"
```

the `icij-worker` lib lets you very simply turn it into a task executed asynchronously by a worker: 

```python
from icij_worker import AsyncApp

app = AsyncApp("my_app")

@app.task
def hello_world() -> str:
    return "Hello world"
```
## Registering tasks with names

The `app` `AsyncApp` instance acts as **task registry**. By default, task are registered using the decorated function name.

!!! tip
    To avoid conflicts and bugs, it's **highly recommended** to register tasks using a name.

    This will **decouple the task name from the Python function name and avoid naming issues** when you renaming/refactoing task functions.

Applying the above tip, we can register our task with the `hello_world` name:
```python
--8<--
hello_world_app.py:hello_world
--8<--
```

## Task arguments

Since tasks are just plain Python function, they support input arguments.

For instance, let's register a new task which greets a user given as a `str`. When no user is provided, let's call it `unknown`:  
```python
--8<--
hello_world_app.py:hello_user
--8<--
```

## Task result

The task result is simply the value returned by the decorated function.

## Supported argument and result types


`icij-worker` tasks support any types as arguments and result **as long as they are JSON serializable**.

!!! tip
       
    If you are used to using libs such as [pydantic](https://docs.pydantic.dev/latest/), your task can have `dict` arguments, which you can easily convert into `pydantic.Model`.
    Similarly, you will have to make sure to convert output into from `pydantic.Model` to a `dict` using the `pydantic.Model.dict` method. 

## Publishing progress updates

For long-running task (longer than the one above), it can be useful to publish progress updates in order to monitor the task execution.

This can be done by adding a  `:::python progress: RateProgress` argument to task functions.
The `progress` **argument name is reserved** and `icij-worker` will automatically populate this argument with a async coroutine that you can call to publish progress updates.
**Progress updates** are expected to be between `0.0` and `1.0` included:

```python
from icij_worker.typing_ import RateProgress

@app.task(name="hello_user_progress")
async def hello_user_progress(user: str | None, progress: RateProgress) -> str:
    greeting = "Hello "
    await progress(0.5)
    if user is None:
        user = "unknown"
    res = greeting + user
    await progress(1)
    return res
```
!!! note
    In practice, `:::python RateProgress = Callable[[float], Awaitable[None]]`, which mean that publishing tasks progress is performed through an `asyncio` coroutine.
    To support publishing progress updates, task functions have to be async. 

    Notice, how `hello_user_progress` is defined using `async def`  
    
## Next

You'll learn see what a full async app looks like !