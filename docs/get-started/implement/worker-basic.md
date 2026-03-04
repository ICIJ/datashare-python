# Basic Datashare worker

## Initialize a worker python package

```console
$ uvx datashare-python project init basic-worker
Initializing basic-worker worker project in .
Project basic-worker initialized !
$ cd basic-worker
```

## Implement an activity function

As seen in the [task tutorial](../../learn/tasks.md#task-arguments), one of the task/activity workflow we can implement 
takes the `:::python user: dict | None` argument automatically added by Datashare to all workflows and greet that user:

```python title="basic_worker/activities.py"
--8<--
basic_activities.py:hello_user_fn
--8<--
```

## Register an activity

Let's register our function as a temporal activity under the `hello_user` name:

```python title="basic_worker/activities.py" hl_lines="4 11"
--8<--
basic_activities.py:activities
--8<--
``` 

1. decorate the activity function with `@activity_defn` using `"hello-user"` as name
2. expose the activity function to `datashare-python`'s CLI, by listing it in the `ACTIVITIES` variable

Under the hood, the `ACTIVITIES` is registered as
[plugin entrypoint](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins)
in the package's `pyproject.toml`:
```toml title="pyproject.toml" 
--8<--
pyproject.toml:entry_points_acts
--8<--
```
When running a worker through `datashare-python worker start`, the CLI will look for any variable registered under 
the `:::toml "datashare.activities"` key and will be able to run the associated activities.

You can register as many variables as you want, under the names of your choices, as long as it's registered under
the `:::toml "datashare.activities"` key.  

## Implement and register a workflow

We've implemented an [activity](https://docs.temporal.io/activities) performing a task but Datashare
runs temporal run [workflows](https://docs.temporal.io/workflows) not activities. 

In our case, the workflow could be as simple as running the `hello_world` activity:
```python
--8<--
basic_run_workflow.py
--8<--
```

In practice, we'll build a
[temporal workflow](https://docs.temporal.io/develop/python/core-application#develop-workflows) to run our activity:

```python title="basic_worker/workflows.py" hl_lines="8 10 13 19"
--8<--
basic_workflows.py:workflows
--8<--
```
    
1. decorate the workflow class with `@workflow.defn` using `"hello-user"` as name  
2. decorate `run`function with `@workflow.run` 
3. execute our `hello_user` activity
4. expose the workflow class to `datashare-python`'s CLI, by listing it in the `:::python WORKFLOWS` variable

Just like for activities, our workflow is exposed to `datashare-python`'s CLI under the `:::python WORKFLOWS` variable,
bound in the `pyproject.toml`:
```toml title="pyproject.toml" 
--8<--
pyproject.toml:entry_points_wfs
--8<--
```

## Next

Now that you have created a basic app, you can either:

- learn how to [build a docker image](../build.md) from it
- learn how to implement a more realistic worker in the [advanced example](./worker-advanced.md)

