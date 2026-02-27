import logging
import uuid
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from copy import deepcopy
from typing import Any, Self, Unpack

from aiohttp import (
    BasicAuth,
    ClientResponse,
    ClientResponseError,
    ClientSession,
    DummyCookieJar,
)
from aiohttp.client import _RequestOptions
from aiohttp.typedefs import StrOrURL

from .exceptions import UnknownTask
from .objects import Task, TaskError, TaskState

logger = logging.getLogger(__name__)

# TODO: move this one to icij_common


class AiohttpClient(AbstractAsyncContextManager):
    def __init__(
        self,
        base_url: str,
        auth: BasicAuth | None = None,
        headers: dict | None = None,
    ):
        self._base_url = base_url
        self._auth = auth
        self._session: ClientSession | None = None
        if headers is None:
            headers = dict()
        self._headers = headers

    async def __aenter__(self):
        self._session = ClientSession(self._base_url, auth=self._auth)
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):  # noqa: ANN001
        await self._session.__aexit__(exc_type, exc_value, traceback)

    @asynccontextmanager
    async def _put(
        self, url: StrOrURL, *, data: Any = None, **kwargs: Any
    ) -> AsyncGenerator[ClientResponse, None]:
        headers = deepcopy(self._headers)
        headers.update(kwargs.pop("headers", dict()))
        logger.debug("PUT headers: %s", headers)  # add this
        logger.debug("Cookie jar: %s", list(self._session.cookie_jar))
        async with self._session.put(url, data=data, headers=headers, **kwargs) as res:
            _raise_for_status(res)
            yield res

    @asynccontextmanager
    async def _get(
        self, url: StrOrURL, *, allow_redirects: bool = True, **kwargs: Any
    ) -> AsyncGenerator[ClientResponse, None]:
        headers = deepcopy(self._headers)
        headers.update(kwargs.pop("headers", dict()))
        async with self._session.get(
            url, headers=headers, allow_redirects=allow_redirects, **kwargs
        ) as res:
            _raise_for_status(res)
            yield res

    @asynccontextmanager
    async def _post(
        self, url: StrOrURL, *, json: Any = None, **kwargs: Any
    ) -> AsyncGenerator[ClientResponse, None]:
        headers = deepcopy(self._headers)
        headers.update(kwargs.pop("headers", dict()))
        async with self._session.post(url, json=json, headers=headers, **kwargs) as res:
            _raise_for_status(res)
            yield res

    @asynccontextmanager
    async def _delete(
        self, url: StrOrURL, **kwargs: Unpack["_RequestOptions"]
    ) -> AsyncGenerator[ClientResponse, None]:
        headers = deepcopy(self._headers)
        headers.update(kwargs.pop("headers", dict()))
        async with self._session.delete(url, headers=headers, **kwargs) as res:
            _raise_for_status(res)
            yield res


def _raise_for_status(res: ClientResponse) -> None:
    try:
        res.raise_for_status()
    except ClientResponseError as e:
        msg = "request to %s, failed with reason %s"
        logger.exception(msg, res.request_info, res.reason)
        raise e


class DatashareTaskClient(AiohttpClient):
    def __init__(self, datashare_url: str, api_key: str | None = None) -> None:
        self._api_key = api_key
        headers = None
        if api_key is not None:
            headers = {"Authorization": f"Bearer {api_key}"}
        super().__init__(datashare_url, headers=headers)

    async def __aenter__(self) -> Self:
        if self._api_key is None:  # Assume no auth necessary
            # Disable aiohttp cookie handling which messes up with the CSRF token stuff
            cookies = DummyCookieJar()
            self._session = ClientSession(
                self._base_url, auth=self._auth, cookies=cookies
            )
            await self._session.__aenter__()
            async with self._get("/settings") as res:  # Get a random route with no auth
                self._session.cookie_jar.clear()
                cookies = {}
                for header_val in res.headers.getall("Set-Cookie", []):
                    cookie_part = header_val.split(";")[0].strip()
                    name, _, value = cookie_part.partition("=")
                    name, value = name.strip(), value.strip('"')
                    cookies[name] = value
                self._headers["Cookie"] = "; ".join(
                    f"{k}={v}" for k, v in cookies.items()
                )
                self._headers["X-DS-CSRF-TOKEN"] = cookies["_ds_csrf_token"]
        else:
            await super().__aenter__()
        return self

    async def create_task(
        self,
        name: str,
        args: dict[str, Any],
        *,
        id_: str | None = None,
        group: str | None = None,
    ) -> str:
        if id_ is None:
            id_ = _generate_task_id(name)
        task = Task(id=id_, name=name, args=args)
        task = task.model_dump(mode="json", exclude_none=True)
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
        task = Task.model_validate(task)
        return task

    async def get_tasks(self) -> AsyncGenerator[Task, None]:
        url = "/api/task"
        async with self._get(url) as res:
            tasks = await res.json()

        while tasks["items"]:
            for t in tasks["items"]:
                yield Task.model_validate(t)
            url = f"/api/task?from={tasks['from']}"
            async with self._get(url) as res:
                tasks = await res.json()

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

    async def delete(self, id_: str) -> None:
        url = f"/api/task/{id_}"
        async with self._delete(url):
            pass

    async def delete_all_tasks(self) -> None:
        async for t in self.get_tasks():
            await self.delete(t.id)


def _generate_task_id(task_name: str) -> str:
    return f"{task_name}-{uuid.uuid4()}"
