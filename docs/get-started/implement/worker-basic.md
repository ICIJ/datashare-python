# Basic Datashare worker

## Initialize a worker python package

<!-- termynal -->
```console
$ uvx datashare-python project init basic-worker
Initializing basic-worker worker project in .
Project basic-worker initialized !
$ cd basic-worker
```

## Implement an activity function

Let's implement a simple activity function taking a `:::python user: dict | None` argument as input and greeting
this user. The function performing this task is:

```python title="basic_worker/activities.py"
--8<--
basic_activities.py:hello_user_fn
--8<--
```

## Register an activity

Next, we'll need to turn our task function into a temporal [activity](https://docs.temporal.io/activities) under the `:::python "hello_user"` name and register 
inside the `:::python ACTIVITIES` variable:

```python title="basic_worker/activities.py" hl_lines="4 11"
--8<--
basic_activities.py:activities
--8<--
``` 

1. decorate the activity function with `@activity_defn` using `"hello-user"` as name
2. expose the activity function to `datashare-python`'s CLI, by listing it in the `ACTIVITIES` variable

Under the hood, the `ACTIVITIES` variable is registered as
[plugin entrypoint](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins)
in the package's `pyproject.toml`:
```toml title="pyproject.toml" 
--8<--
pyproject.toml:entry_points_acts
--8<--
```
When running a worker using `datashare-python worker start` CLI, `datashare-python` will look for any variable registered under 
the `:::toml "datashare.activities"` key and will be able to run activities registered in these variables.

You can register as many variables as you want, under the names of your choices, as long as it's bound under
the `:::toml "datashare.activities"` key.  

## Implement and register a workflow

Temporal [activities](https://docs.temporal.io/activities) run inside [workflows](https://docs.temporal.io/workflows). In our case the workflow takes `:::python args: dict` as input
(like all Datashare workflows) and just run our our `hello_world` activity function. In plain Python code, this is:
```python
--8<--
basic_run_workflow.py
--8<--
```

In practice, we need to turn our workflow function into an actual [temporal workflow](https://docs.temporal.io/develop/python/core-application#develop-workflows) like so:

```python title="basic_worker/workflows.py" hl_lines="8 10 12 14 20"
--8<--
basic_workflows.py:workflows
--8<--
```
    
1. decorate the workflow class with `@workflow.defn` using `"hello-user"` as name  
2. decorate `run`function with `@workflow.run` 
3. get the `:::python user` from workflow's args  
4. execute our `hello_user` activity with the user
5. expose the workflow class to `datashare-python`'s CLI, by listing it in the `:::python WORKFLOWS` variable

Just like for activities, our workflow is exposed to `datashare-python`'s CLI under the `:::python WORKFLOWS` variable,
bound in the `pyproject.toml`:
```toml title="pyproject.toml" 
--8<--
pyproject.toml:entry_points_wfs
--8<--
```

We've built a very simple workflows, but in practice, real workflows can be arbitrarily complex.

## Next

Now that you have created a basic app, you can either:

- learn how to [build a docker image](../build.md) from it
- learn how to implement a more realistic worker in the [advanced example](./worker-advanced.md)

