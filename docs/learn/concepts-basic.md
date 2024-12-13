# Basic concepts and definitions

Before starting, here are a few definitions of concepts that we'll regularly use in this documentation.

The following concepts are important for the rest of this tutorial, make sure you understand them properly !

## Definitions

### Tasks

***Tasks*** (a.k.a. *"async tasks"* or *"asynchronous tasks"*) are **units of work** that can be
executed [asynchronously](#asynchronous).

Datashare has its own **built-in** tasks such as indexing documents, finding named entities, performing search or
download by batches... Tasks are visible in on [Datashares's tasks page](https://datashare-demo.icij.org/#/tasks).

The goal of this documentation is to let you implement **your own custom tasks**. They could virtually be anything:

- classifying documents
- extracting named entities from documents
- extracting structured content from documents
- translating documents
- tagging documents
- ...

### Asynchronous

In our context ***asynchronous*** mean *"executed in the background"*. Since tasks can be long, getting their result is
not as simple as calling an API endpoint.

Instead, executing a task asynchronously implies:

1. requesting the task execution by publish the task name and arguments (parameters needed to perform the task) on
   the [broker](./concepts-advanced#broker)
2. receive the task name and arguments from the broker and perform the actual task in the background inside
   a [task worker](#workers) (optionally publishing progress updates on the broker)
3. monitor the task progress
4. saving task results or errors
5. accessing the task results or errors

### Workers

***Workers*** (a.k.a. *"async apps"*) are infinite loop **Python programs running [async tasks](#tasks)**.

They pseudo for the worker loop is:

```python
while True:
    task_name, task_args = get_next_task()
    task_fn = get_task_function_by_name(task_name)
    try:
        result = task_fn(**task_args)
    except Exception as e:
        save_error(e)
        continue
    save_result(result)
```

### Task Manager

The ***task manager*** is the primary interface to interact with tasks. The task manager lets us:

- create [tasks](#tasks) and send them to [workers](#workers)
- post task [task state](concepts-advanced.md#task-states) and progress updates
- monitor [task state](concepts-advanced.md#task-states) and progress
- get task results and errors
- cancel task
- ...

### App

***Apps*** (a.k.a. *"async apps"*) are **collections of [tasks](#tasks)**, they act as a registry and bind a task name
to an actual unit of work (a.k.a. a Python function).

## Next

- skip directly to learn more about [tasks](tasks.md)
- or continue to learn about [advanced concepts](concepts-advanced.md)