import uuid
from typing import Any, Dict, Optional

from icij_common.pydantic_utils import jsonable_encoder
from icij_worker import Task, TaskError, TaskState
from icij_worker.exceptions import UnknownTask
from icij_worker.utils.http import AiohttpClient

# TODO: maxRetries is not supported by java, it's automatically set to 3
_TASK_UNSUPPORTED = {"max_retries"}


class DatashareTaskClient(AiohttpClient):
    def __init__(self, datashare_url: str, api_key: str | None = None) -> None:
        headers = None
        if api_key is not None:
            headers = {"Authorization": f"Bearer {api_key}"}
        super().__init__(datashare_url, headers=headers)

    async def __aenter__(self):
        await super().__aenter__()
        if "Authorization" not in self._headers:
            async with self._get("/settings") as res:
                # SimpleCookie doesn't seem to parse DS cookie so we perform some dirty
                # hack here
                session_id = [
                    item
                    for item in res.headers["Set-Cookie"].split("; ")
                    if "session_id" in item
                ]
                if len(session_id) != 1:
                    raise ValueError("Invalid cookie")
                k, v = session_id[0].split("=")
                self._session.cookie_jar.update_cookies({k: v})

    async def create_task(
        self,
        name: str,
        args: Dict[str, Any],
        *,
        id_: Optional[str] = None,
        group: Optional[str] = None,
    ) -> str:
        if id_ is None:
            id_ = _generate_task_id(name)
        task = Task.create(task_id=id_, task_name=name, args=args)
        task = jsonable_encoder(task, exclude=_TASK_UNSUPPORTED, exclude_unset=True)
        task.pop("createdAt")
        url = f"/api/task/{id_}"
        if group is not None:
            if not isinstance(group, str):
                raise TypeError(f"expected group to be a string found {group}")
            url += f"?group={group}"
        async with self._put(url, json=task) as res:
            task_res = await res.json()
        return task_res["taskId"]

    async def get_task(self, id_: str) -> Task:
        url = f"/api/task/{id_}"
        async with self._get(url) as res:
            task = await res.json()
        if task is None:
            raise UnknownTask(id_)
        # TODO: align Java on Python here... it's not a good idea to store results
        #  inside tasks since result can be quite large and we may want to get the task
        #  metadata without having to deal with the large task results...
        task = _ds_to_icij_worker_task(task)
        task = Task(**task)
        return task

    async def get_tasks(self) -> list[Task]:
        url = "/api/task/all"
        async with self._get(url) as res:
            tasks = await res.json()
        # TODO: align Java on Python here... it's not a good idea to store results
        #  inside tasks since result can be quite large and we may want to get the task
        #  metadata without having to deal with the large task results...
        tasks = (_ds_to_icij_worker_task(t) for t in tasks)
        tasks = [Task(**task) for task in tasks]
        return tasks

    async def get_task_state(self, id_: str) -> TaskState:
        return (await self.get_task(id_)).state

    async def get_task_result(self, id_: str) -> Any:
        url = f"/api/task/{id_}/results"
        async with self._get(url) as res:
            task_res = await res.json()
        return task_res

    async def get_task_error(self, id_: str) -> TaskError:
        url = f"/api/task/{id_}"
        async with self._get(url) as res:
            task = await res.json()
        if task is None:
            raise UnknownTask(id_)
        task_state = TaskState[task["state"]]
        if task_state != TaskState.ERROR:
            msg = f"can't find error for task {id_} in state {task_state}"
            raise ValueError(msg)
        error = TaskError(**task["error"])
        return error

    async def delete(self, id_: str):
        url = f"/api/task/{id_}"
        async with self._delete(url):
            pass

    async def delete_all_tasks(self):
        for t in await self.get_tasks():
            await self.delete(t.id)


def _generate_task_id(task_name: str) -> str:
    return f"{task_name}-{uuid.uuid4()}"


_JAVA_TASK_ATTRIBUTES = ["result", "error"]


def _ds_to_icij_worker_task(task: dict) -> dict:
    for k in _JAVA_TASK_ATTRIBUTES:
        task.pop(k, None)
    return task
