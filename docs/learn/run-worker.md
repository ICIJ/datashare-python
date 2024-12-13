# Running a worker pool with the [`icij-worker`](https://github.com/ICIJ/icij-python/tree/main/icij-worker) CLI

In the previous [section](app.md), we've created a simple async app featuring multiple tasks.

We now need start a [worker pool](./concepts-advanced.md#worker-pools), in order to run the app.
Workers will execute the app tasks when new tasks are published by the [task manager](./concepts-basic.md#task-manager) through the [broker](./concepts-advanced.md#broker).

## Installing the `icij-worker` CLI

Once your app is created you can use your favorite package manager to install [`icij-worker`](https://github.com/ICIJ/icij-python/tree/main/icij-worker):
<!-- termynal -->
```
$ python -m pip install icij-worker 
```


## Start the worker pool

To start a worker pool we'll need to provide a config. Config can be provided via environment variables or using a config files.
Check out our [worker config guide](../guides/worker-config) in order to learn about all config parameters as well as env var naming conventions.  

### Using environment variables 

Since we've put all of our app code in a variable named `my_app` in a file named `app.py`, we can start the app as following:
<!-- termynal -->
```
$ ICIJ_WORKER_TYPE=amqp python -m icij-worker workers start -n 2 app.app
[INFO][icij_worker.backend.backend]: Loading worker configuration from env...
[INFO][icij_worker.backend.backend]: worker config: {
  ...
}
[INFO][icij_worker.backend.mp]: starting 2 worker for app my_app.app
... 
```
!!! note
    The above code assumes that an AMQP [broker](./concepts-advanced.md#broker) is running on the default host and ports.
    Check out the [broker config guide](../guides/broker-config) to make sure your worker can correctly connect to it.

Let's break this down a bit
```bash
$ ICIJ_WORKER_TYPE=amqp python -m icij-worker workers start -n 2 app.app
 
```

### Using a config file

Sometimes it can be convenient to configure the worker pool via a config file rather than env vars,
in this case you can just drop your config into a file:

```json title="app_config.json"
{
  "type": "amqp"
}
```
and start the pool like this:
<!-- termynal -->
```
$ python -m icij-worker workers start -c app_config.json -n 2 app.app
[INFO][icij_worker.backend.backend]: Loading worker configuration from env...
[INFO][icij_worker.backend.backend]: worker config: {
  ...
}
[INFO][icij_worker.backend.mp]: starting 2 worker for app my_app.app
...
```

## Next

Here, we've built a basic app and run it using a worker pool.
However, the app we've built is not able yet to speak with Datashare's [Task Manager](concepts-basic.md#task-manager).

Continue this tutorial to learn how implement task which can be integrated inside Datashare !