# Advanced concepts and definitions

The following concepts are not required to understand what's following, you get back to them as need. 

### Worker pools
A ***worker pool*** is a collection of [task workers](./concepts-basic.md#workers) running the **same [async app](./concepts-basic.md#app)** on the **same machine**.

A worker pool can be created and started using the [`icij-worker`](https://github.com/ICIJ/icij-python/tree/main/icij-worker) CLI. 

### Broker

We call ***broker*** the messaging service and protocol allowing Datashare's [task manager](./concepts-basic.md#task-manager) and [task workers](./concepts-basic.md#workers) to communicate together.   

Behind the scene we use a custom task protocol built on the top of [RabbitMQ](https://www.rabbitmq.com/)'s [AMQP protocol](https://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol).

### Queues
***Queues*** are stacks of messages sent over the [broker](#broker). It's possible to route messages to specific queues so that it's read by some specific agents ([task manager](./concepts-basic.md#task-manager) or some specific [worker](./concepts-basic.md#workers)).

While there can be several workers, **there is single task manager**, and it's already running inside Datashare's backend, so most of the time you don't have to care about it !

### Task States

A task can have different ***task states***:
```python
class TaskState(str, Enum):
    # When created through the TaskManager
    CREATED = "CREATED"
    # When published to some queue, but not grabbed by a worker yet
    QUEUED = "QUEUED"
    # When grabbed by and executed by a worker
    RUNNING = "RUNNING"
    # When the worker couldn't execute the task for some reason
    ERROR = "ERROR"
    # When successful
    DONE = "DONE"
    # When cancelled by the user
    CANCELLED = "CANCELLED"
```

### Task Groups

Because its can be convenient to implement task meant to be executed by different types of [workers](./concepts-basic.md#workers) in the same [app](./concepts-basic.md#app),
tasks can be grouped together to be executed by a given type of worker. 

In an ML context it's frequent to perform preprocessing tasks which require memory and CPU, and then perform ML inference which require GPU. 
Because, it's convenient to implement all these tasks as part of the same codebase (in particular for testing) and register them in the same [app](./concepts-basic.md#app), task can be assigned to a group.

When starting a worker it's possible to provide a group of tasks to be executed. In the above case, we could for instance define a `cpu` and a `gpu` groups and split task between them.