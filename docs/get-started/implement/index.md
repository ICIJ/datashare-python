# How to use the worker template repo ?

The [datashare-python](https://github.com/ICIJ/datashare-python) repository is meant to be used as a template to implement your own Datashare worker.

## Clone the template repository

Start by cloning the [template repository](https://github.com/ICIJ/datashare-python):

<!-- termynal -->
```console
$ git clone git@github.com:ICIJ/datashare-python.git
---> 100%
```

## Explore the codebase

In addition to be used as a template, the repository can also showcases some of advanced schemes detailed in the
[guides](../../guides/index.md) section of this documentation.

Don't hesitate to have a look at the codebase before starting (or get back to it later on) !

In particular the following files should be of interest:
```console
.
├── ml_worker
│         ├── app.py
│         ├── config.py
│         ├── tasks
│         │         ├── __init__.py
│         │         ├── classify_docs.py
│         │         ├── dependencies.py
│         │         └── translate_docs.py
```


## Replace existing tasks with your own

To implement your Datashare worker the only thing you have to do is to **replace existing tasks with your own and
register them in the `app` app variable of the `app.py` file.**

We'll detail how to do so in the [Basic Worker](./worker-basic.md) and [Advanced Worker](./worker-advanced.md) examples.