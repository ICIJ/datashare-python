# Start tasks

Currently, it's you can interact with Datashare tasks using the `/api/task/*` 
[HTTP APIs route of Datashare](https://icij.gitbook.io/datashare/developers/backend/api).

With Datashare and your worker running, you can now use the `datashare-python` CLI for that:

[//]: # (TODO: add the output)
<!-- termynal -->
```console
$ uvx datashare-python task --help
```

## Basic worker

Let's create a task, monitor it and get its result:

[//]: # (TODO: add the output)
[//]: # (TODO: add the task id)
<!-- termynal -->
```console
$ uvx datashare-python task start hello_world
$ uvx datashare-python task watch  <>
$ uvx datashare-python task result  <>
```

## Advanced worker

First let's create
[//]: # (TODO: add the output)
[//]: # (TODO: add the task id)
<!-- termynal -->
```console
$ uvx datashare-python task create hello_world
$ uvx datashare-python task watch  <>
$ uvx datashare-python task result  <>
```
