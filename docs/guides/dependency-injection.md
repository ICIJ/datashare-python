# Dependency injection

## What are dependencies ?

We call "lifepsan dependency" or just "dependency" any object used by an activity workers and which needs to live longer
then the activity duration.

Common dependencies include:
- clients
- connections and connection pools
- ML models

Dependency injection make sure that these dependencies are **available** in your activity code and also makes sure
**resources are correctly freed** when they are no longer needed.

## How `datashare-python` dependency injection works ?

`datashare-python` dependency injection is inspired by [fastapi](https://fastapi.tiangolo.com/advanced/events/)'s 
lifespan events handling.

The idea is to:
1. provide an number of functions and/or context managers initializing dependencies and storing them in the worker
thread [context](https://docs.python.org/3/library/contextvars.html)
2. define functions access this context and letting you access these variables in your code

## Providing dependencies
 
If you are building an automatic speech recognition worker, you might implement the following activity:
```python title="activities.py"
--8<--
naive_asr_activity.py
--8<--
```

1. this is awfully heavy, we don't want to reload the model everytime !

The obvious problem with this implementation is that we'll reload the model each time we receive audios to process.
Ideally we'd like to have the model **preloaded in memory and just run inference**.

Instead, we'll define a lifespan dependency which loads the model and stores it into the worker thread content
variables:

```python title="dependency.py"
--8<--
naive_dependencies.py
--8<--
```

1. register a context variable with the `ml_model` name
2. load the model
3. store the model into the registered context variable

A **better version** of this dependency uses context manager to **make sure resource are freed** when worker no longer
needs the dependency:  

```python title="dependency.py"
--8<--
dependencies.py:context_manager
--8<--
```

1. let the calling code run
2. clean everything up when the caller is done

## Accessing dependencies

Now that we've registered our dependency in the thread context, we need to update our activity to access the context
variable. We can do it directly by calling `:::python ContextVar("ml_model").get()`, but we can more elegantly define the
following dependency function:

```python title="dependency.py"
--8<--
dependencies.py:expose_dependency
--8<--
``` 

Next, we'll use this function in our activity:
```python title="dependency.py"
--8<--
asr_activity.py
--8<--
``` 

1. load cached model rather than reloading it

## Worker dependency discovery

In order for dependencies to by discoverable by `datashare-python`'s CLI, they need to be registered.

```python title="dependency.py"
--8<--
dependencies.py:registry
--8<--
```

Under the hood, the `DEPENDENCIES` variable is registered as [plugin entrypoint](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins)

```toml title="pyproject.toml"
--8<--
pyproject.toml:entry_points_deps
--8<--
``` 

When running a worker using `datashare-python worker start` CLI, `datashare-python` will look for any variable registered under 
the `:::toml "datashare.dependencies"` key and the `dependencies` entry point name and will be able to run activities registered in these variables.

You can register as dependency sets as you want in the bounded variable. You can use the variable name of your choice
for the dict registry, as long as it's bound under the `:::toml "datashare.dependencies"` key the `:::toml dependencies` entry point name.
****

## Selecting dependencies when running `datashare-python`'s CLI

When running an activity worker using

<!-- termynal -->
```console
datashare-python worker start --activities asr-transcription 
```

the `datashare-python` will auto discover dependencies and if the registry has a single entry in it, it will
automatically use this dependency sets.

In case your registry contains multiple dependency sets, you can provide call the CLI providing the set's key (here `:::python "base"`) as argument:

<!-- termynal -->
```console
datashare-python worker start --activities asr-transcription --dependencies base 
```
