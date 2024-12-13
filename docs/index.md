<style>
.md-content .md-typeset h1
</style>

<p align="center">
  <a href="https://datashare.icij.org/">
    <img align="center" src="assets/datashare-logo.svg" alt="Datashare" style="max-width: 60%">
  </a>
</p>
<p align="center">
    <em>Better analyze information, in all its forms</em>
</p>
<br/>

# Implement your own Datashare tasks, written in Python

Most AI, Machine Learning, Data Engineering happens in Python.
[Datashare](https://icij.gitbook.io/datashare) now lets you extend its backend with your own tasks implemented in Python.

Turning your own ML pipelines into Datashare tasks is **very simple**.

Actually, it's *almost* as simple as cloning our [template repo](https://github.com/ICIJ/datashare-ml-worker-template):

<!-- termynal -->
```
$ git clone git@github.com:ICIJ/datashare-ml-worker-template.git
---> 100%
```

replacing existing [app](https://github.com/ICIJ/datashare-ml-worker-template/blob/main/ml_worker/app.py) tasks with your own:   
```python
--8<--
hello_world.py
--8<--
```

installing [`uv`](https://docs.astral.sh/uv/) to install dependencies and running your async Datashare worker:
<!-- termynal -->
```
$ cd datashare-ml-worker-template
$ curl -LsSf https://astral.sh/uv/install.sh | sh
---> 100%
$ uv run ./scripts/worker_entrypoint.sh
[INFO][10:26:52.063][icij_worker.backend.backend]: Loading worker configuration from env...
[INFO][10:26:52.065][icij_worker.backend.backend]: worker config: {
  "connection_timeout_s": 5.0,
  "reconnection_wait_s": 5.0,
  "rabbitmq_host": "localhost",
  "rabbitmq_password": "*****",
  "rabbitmq_port": 5672,
  "rabbitmq_management_port": 15672,
  "rabbitmq_user": "guest",
  "rabbitmq_vhost": "%2F",
  "rabbitmq_is_qpid": false,
  "app_bootstrap_config_path": null,
  "inactive_after_s": null,
  "log_level": "INFO",
  "task_queue_poll_interval_s": 1.0
}
[INFO][10:26:52.065][icij_worker.backend.mp]: starting 1 worker for app ml_worker.app.app
... 
```
you'll then be able to execute task by starting using our [HTTP client]() (and soon using Datashare's UI).    

[//]: # (TODO: add a link to the HTTP task creation guide)

## Learn

Learn how to integrate Data Processing and Machine Learning pipelines to Datashare following our [tutorial](./learn/tasks.md). 

## Get started

Follow our [get started](get-started/index.md) guide an learn how to clone the [template repository](https://github.com/ICIJ/datashare-ml-worker-template) and implement your own Datashare tasks !

## Refine your knowledge
 
Follow our [guides](guides/index.md) to learn how to implement complex tasks and deploy Datashare workers running your own tasks.