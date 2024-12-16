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

## Defining tasks using the `PYTHON` task group 

## Running a group specify worker pool
