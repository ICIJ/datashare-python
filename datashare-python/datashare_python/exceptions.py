from abc import ABC


class DatashareError(Exception, ABC): ...


class UnknownTask(DatashareError, ValueError):
    def __init__(self, task_id: str, worker_id: str | None = None):
        msg = f'Unknown task "{task_id}"'
        if worker_id is not None:
            msg += f" for {worker_id}"
        super().__init__(msg)


class DependencyInjectionError(DatashareError, RuntimeError):
    def __init__(self, name: str):
        msg = f"{name} was not injected"
        super().__init__(msg)
