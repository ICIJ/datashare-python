# Start tasks

Currently, it's you can interact with Datashare tasks using the `/api/task/*` 
[HTTP APIs route of Datashare](https://icij.gitbook.io/datashare/developers/backend/api).

With Datashare and your worker running, you can now use the `datashare-python` CLI for that:

[//]: # (TODO: add the output)
<!-- termynal -->
```console
$ uvx datashare-python tasks --help
```

## Basic worker

Let's create a task, monitor it and get its result:

[//]: # (TODO: add the output)
[//]: # (TODO: add the task id)
<!-- termynal -->
```console
$ uvx datashare-python tasks create hello_world
$ uvx datashare-python tasks watch  <>
$ uvx datashare-python tasks result  <>
```

## Advanced worker

First let's create
[//]: # (TODO: add the output)
[//]: # (TODO: add the task id)
<!-- termynal -->
```console
$ uvx datashare-python tasks create hello_world
$ uvx datashare-python tasks watch  <>
$ uvx datashare-python tasks result  <>
```
