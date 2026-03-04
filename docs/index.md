<style>
.md-content .md-typeset h1
</style>

<div style="background-image: linear-gradient(45deg, #193d87, #fa4070);">
  <br/>
  <p align="center">
    <a href="https://datashare.icij.org/">
      <img align="center" src="assets/datashare-logo.svg" alt="Datashare" style="max-width: 60%;">
    </a>
  </p>
  <p align="center">
    <em>Better analyze information, in all its forms</em>  
  </p>
  <br/>
</div>
<br/>
# Implement **your own Datashare tasks**, written in Python

Most AI, Machine Learning, Data Engineering happens in Python.
[Datashare](https://icij.gitbook.io/datashare) now lets you extend its backend with your own tasks implemented in Python.

Turning your own data processing pipelines into a Datashare worker running your pipeline is very simple.

Let's turn this dummy pipeline function into a Datashare worker:
```py
--8<--
activities.py:5:7
--8<--
```
 

We start by running the [`datashare-python`](https://github.com/ICIJ/datashare-python)'s CLI to create a worker project from a template:  

=== "Linux, MacOS"
    <!-- termynal -->
    ```console
    $ curl -LsSf https://astral.sh/uv/install.sh | sh
    ---> 100%
    $ uvx datashare-python project init hello-world
    Initializing hello-world worker project in .
    Project hello-world initialized !
    $ cd hello-world
    ```
=== "Windows"
    <!-- termynal -->
    ```console
    $ powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ---> 100%
    $ uvx datashare-python project init hello-world
    Initializing hello-world worker project in .
    Project hello-world initialized !
    $ cd hello-world
    ```

Datashare's asynchronous execution is backed by the [temporal](https://docs.temporal.io/develop/python/) durable
execution framework.
In temporal [workflows](https://docs.temporal.io/workflows) are described in
plain Python code in which some tasks ([activities](https://docs.temporal.io/activities) in Temporal's terminology) are executed.

Then we implement a simple `HelloWorld` workflow, running a single `hello` activity. Here is what our new activity should look like:
```py title="hello_world/activities.py"
--8<--
activities.py
--8<--
```

Next, we integrate our task/activity into a workflow:
```py title="hello_world/workflows.py"
--8<--
workflows.py
--8<--
```

Finally we set up dependencies and run our async Datashare worker:

<div class="wrap">
<!-- termynal -->
```python
$ uv run --frozen datashare-python worker start --activities hello --workflows hello-world --queue hello
```
</div>


you'll then be able to execute workflow using the `datashare-python` CLI.    

[//]: # (TODO: add a link to the HTTP task creation guide)

## **Learn**

Learn how to integrate Data Processing and Machine Learning pipelines to Datashare following our [tutorial](./learn/tasks.md). 

## **Get started**

Follow our [get started](get-started/index.md) guide an learn how to clone the [template repository](https://github.com/ICIJ/datashare-python) and implement your own Datashare tasks !

## **Refine your knowledge**
 
Follow our [guides](guides/index.md) to learn how to implement complex tasks and deploy Datashare workers running your own tasks.
