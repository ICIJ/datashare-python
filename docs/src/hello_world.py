from icij_worker import AsyncApp

app = AsyncApp("some-app")


@app.task
def hello_world() -> str:
    return "Hello world"
