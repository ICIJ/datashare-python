from icij_worker import AsyncApp

app = AsyncApp("app")


@app.task
def hello_world() -> str:
    # This is
    return "Hello world"
