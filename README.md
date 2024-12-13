<style>
.md-content .md-typeset h1
</style>

<p align="center">
  <a href="https://datashare.icij.org/">
    <img align="center" src="docs/assets/datashare-logo.svg" alt="Datashare" style="max-width: 60%">
  </a>
</p>
<p align="center">
    <em>Better analyze information, in all its forms</em>
</p>
<br/>

---

**Documentation**: <a href="https://icij.github.io/datashare-python" target="_blank">https://icij.github.io/datashare-python</a>

---

# Implement **your own Datashare tasks**, written in Python

Most AI, Machine Learning, Data Engineering happens in Python.
[Datashare](https://icij.gitbook.io/datashare) now lets you extend its backend with your own tasks implemented in Python.

Turning your own ML pipelines into Datashare tasks is **very simple**, learn about it inside [documentation](https://icij.github.io/datashare-python).

Turning your own ML pipelines into Datashare tasks is **very simple**.

Actually, it's *almost* as simple as cloning our [template repo](https://github.com/ICIJ/datashare-python):

```
$ git clone git@github.com:ICIJ/datashare-python.git
```

replacing existing [app](https://github.com/ICIJ/datashare-python/blob/main/ml_worker/app.py) tasks with your own:   
```python
from icij_worker import AsyncApp

app = AsyncApp("app")


@app.task
def hello_world() -> str:
    return "Hello world"
```

installing [`uv`](https://docs.astral.sh/uv/) to set up dependencies and running your async Datashare worker:
```console
$ cd datashare-python
$ curl -LsSf https://astral.sh/uv/install.sh | sh
$ uv run ./scripts/worker_entrypoint.sh
[INFO][icij_worker.backend.backend]: Loading worker configuration from env...
...
}
[INFO][icij_worker.backend.mp]: starting 1 worker for app ml_worker.app.app
...
```
you'll then be able to execute task by starting using our [HTTP client]() (and soon using Datashare's UI).

## Learn more reading our [documentation](https://icij.github.io/datashare-python) !