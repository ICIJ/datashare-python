from temporalio import activity


@activity.defn(name="hello")
def hello(person: str) -> str:
    return f"Hello {person}"
