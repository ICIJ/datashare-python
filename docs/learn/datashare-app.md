# Create tasks for Datashare

In this section, you'll learn how to update our [app](./app.md) to run together with Datashare.

## Communicating with Datashare's via AMQP

Datashare features its own [task manager](concepts-basic.md#task-manager) used to execute many built-in tasks.
This task manager can be [configured](../guides/config/config-files-and-env-vars.md) in many different ways and in particular it can be configured to use [AMQP](https://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol) via [RabbitMQ](https://www.rabbitmq.com/) to manage tasks.

Having Datashare's task manager powered by AMQP allows us to integrate our **custom tasks** in Datashare's backend:

- we'll use Datashare's task manager to create custom tasks
- the task manager will publish custom tasks on the AMQP [broker](concepts-advanced.md#broker)
- our Python [worker](concepts-basic.md#workers) will receive the tasks and execute them
- the workers will return their results to the task manager

The above workflow however requires the worker to be properly configured to communicate with Datashare's task manager.

## Aligning worker's config with Datashare

If Datashare uses non default RabbitMQ config make sure to update your config to match Datashare's RabbitMQ config.

Here is how to do so via the config file:
```json title="app_config.json"
{
  "type": "amqp",
  "rabbitmq_host":  "your-rabbitmq-host",
  "rabbitmq_management_port": 16666,
  "rabbitmq_password": "******",
  "rabbitmq_port": 6666,
  "rabbitmq_user":  "your-rabbitmq-user",
  "rabbitmq_vhost": "somevhost"
}
```

If you prefer using env vars, you can update the above variables by setting the `ICIJ_WORKER_<KEY>` variables where`<KEY>` is one of the above JSON's keys. 

Read the full guide to [worker config](../guides/config/worker-config.md) to learn more.

## Grouping our tasks in the `PYTHON` task group

Inside Datashare, tasks can be grouped together in order to be executed by specific workers.
In particular, Datashare defines the `JAVA` task group reserved for built-in tasks
(such as indexing, batch searches/downloads...) and the `PYTHON` task group reserved for your custom tasks.

When tasks are created and published inside Datashare by the [task manager](../learn/concepts-basic.md#task-manager),
it's possible to assign them with a group, allowing the task to be routed to specific workers.

When creating publishing tasks with the `PYTHON` group, this will ensure only your workers will receive the tasks (and not builtin Java workers). 

If you don't use the `PYTHON` task group your worker will receive the builtin tasks reserved for Java workers,
since they can't execute these tasks, they will fail.

In order, to assign a task to a specific group, you can use the `:::python group: str | TaskGroup | None ` argument of the `:::python task` decorator.

Our app can hence be updated as following:

```python title="my_app.py" hl_lines="7 10 15 23"
--8<--
hello_world_app_ds.py:app
--8<--
```

Read the full guide to [task routing](../guides/task-routing.md) to learn more.

## Running group-specific workers

As detailed in the [task routing](../guides/task-routing.md) guide, worker pools can be restricted to execute tasks of a given group.

We can hence start our `PYTHON` worker pool adding the `:::console -g PYTHON` argument:
<!-- termynal -->
```console hl_lines="1"
$ python -m icij-worker workers start -c app_config.json -g PYTHON -n 2 app.app
[INFO][icij_worker.backend.backend]: Loading worker configuration from env...
[INFO][icij_worker.backend.backend]: worker config: {
  ...
}
[INFO][icij_worker.backend.mp]: starting 2 worker for app my_app.app
...
```

