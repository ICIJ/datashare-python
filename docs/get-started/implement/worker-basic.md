# Basic Datashare worker

## Install dependencies

Start by installing [`uv`](https://docs.astral.sh/uv/getting-started/installation/) and dependencies:
<!-- termynal -->
```console
$ curl -LsSf https://astral.sh/uv/install.sh | sh
$ uv sync --frozen --group dev
```

## Implement the `hello_user` task function

As seen in the [task tutorial](../../learn/tasks.md#task-arguments), one of the dummiest tasks we can implement take
the `:::python user: dict | None` argument automatically added by Datashare to all tasks and greet that user.

The function performing this task is the following 

```python
--8<--
basic_app.py:hello_user_fn
--8<--
```

## Register the `hello_user` task

In order to turn our function into a Datashare [task](../../learn/concepts-basic.md#tasks), we have to register it into the 
`:::python app` [async app](../../learn/concepts-basic.md#app) variable of the
[app.py](https://github.com/ICIJ/datashare-python/blob/main/datashare_python/app.py) file, using the `:::python @task` decorator.

Since we won't use existing tasks, we can also perform some cleaning and get rid of them.
The `app.py` file should hence look like this:

```python title="app.py" hl_lines="9"
--8<--
basic_app.py:app
--8<--
```

The only thing we had to do is to use the `:::python @app.task` decorator and make sure to provide it with
`:::python name` to **bind the function to a task name** and group the task in the `:::python PYTHON_TASK_GROUP = TaskGroup(name="PYTHON")`.

As detailed in [here](../../learn/datashare-app.md#grouping-our-tasks-in-the-python-task-group), using this task group
ensures that when custom tasks are published for execution, they are correctly routed to your custom Python worker and
not to the Java built-in workers running behind Datashare's backend. 

## Get rid of unused codebase

## Next

Now that you have created a basic app, you can either:

- learn how to [build a docker image](../build.md) from it
- learn how to implement a more realistic worker in the [advanced example](./worker-advanced.md)
