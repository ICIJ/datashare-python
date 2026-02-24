<div style="background-image: linear-gradient(45deg, #193d87, #fa4070);">
  <br/>
  <p align="center">
    <a href="https://datashare.icij.org/">
      <img align="center" src="docs/assets/datashare-logo.svg" alt="Datashare" style="max-width: 60%">
    </a>
  </p>
  <p align="center">
    <em>Better analysis in all of its forms</em>  
  </p>
  <br/>
</div>
<br/>

---

# Python workers for Temporal in Datashare

This project serves as a repository of Temporal workers and workflows written in Python
(useful in machine learning) for use with [Datashare](https://icij.gitbook.io/datashare). Install with 

```
make install
```

## File patterns

To create new workers, you can follow `asr_worker` with the file/dir structure
```
activities.py --> Workflow activities
constants.py  --> Worker/workflow constants
models.py     --> Workflow and activity inputs/outputs and other data classes
worker.py     --> Worker definition
workflow.py   --> Workflow definition
```

## Docker

Use `docker-compose` to run the dev server on `localhost`, which will start `elasticsearch`
(port `9200`), `postgres` (`5432`), and `redis` (`6379`) services, as well as the `Temporal`
server and ui (`7233` and `8233`), and `datashare` (`8080`). Note that container build and
startup times can be long if workers and workflows rely on large models, so allocate memory
to Docker accordingly.