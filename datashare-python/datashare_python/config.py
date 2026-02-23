from typing import ClassVar

from icij_common.es import ESClient
from icij_common.pydantic_utils import ICIJSettings
from pydantic import Field, PrivateAttr
from pydantic_settings import SettingsConfigDict
from temporalio.contrib.pydantic import pydantic_data_converter

import datashare_python

from .objects import BaseModel
from .task_client import DatashareTaskClient
from .types_ import TemporalClient
from .utils import LogWithWorkerIDMixin

_ALL_LOGGERS = [datashare_python.__name__]

DS_WORKER_SETTINGS_CONFIG = SettingsConfigDict(
    env_prefix="DS_WORKER_",
    env_nested_delimiter="__",
    nested_model_default_partial_update=True,
)


class ESClientConfig(BaseModel):
    address: str = "http://localhost:9200"
    default_page_size: int = 1000
    keep_alive: str = "10m"
    max_concurrency: int = 5
    max_retries: int = 0
    max_retry_wait_s: int | float = 60
    timeout_s: int | float = 60 * 5

    def to_es_client(self, api_key: str | None = None) -> ESClient:
        client = ESClient(
            hosts=[self.address],
            pagination=self.default_page_size,
            max_concurrency=self.max_concurrency,
            keep_alive=self.keep_alive,
            timeout=self.timeout_s,
            max_retries=self.max_retries,
            max_retry_wait_s=self.max_retry_wait_s,
            api_key=api_key,
        )
        client.transport._verified_elasticsearch = True
        return client


class DatashareClientConfig(BaseModel):
    api_key: str | None = None
    url: str = "http://datashare:8080"

    def to_task_client(self) -> DatashareTaskClient:
        return DatashareTaskClient(self.url, self.api_key)


class TemporalClientConfig(BaseModel):
    host: str = "temporal:7233"
    namespace: str = "datashare-default"
    _client: TemporalClient | None = PrivateAttr(default=None)

    async def to_client(self) -> TemporalClient:
        if self._client is None:
            self._client = await TemporalClient.connect(
                target_host=self.host,
                namespace=self.namespace,
                data_converter=pydantic_data_converter,
            )
        return self._client

    # For the lru_cache
    def __hash__(self) -> int:
        return id(self)


class WorkerConfig(ICIJSettings, LogWithWorkerIDMixin, BaseModel):
    model_config = DS_WORKER_SETTINGS_CONFIG

    loggers: ClassVar[list[str]] = Field(_ALL_LOGGERS, frozen=True)
    log_level: str = Field(default="INFO")

    datashare: DatashareClientConfig = DatashareClientConfig()
    elasticsearch: ESClientConfig = ESClientConfig()
    temporal: TemporalClientConfig = TemporalClientConfig()

    def to_es_client(self) -> ESClient:
        return self.elasticsearch.to_es_client(self.datashare.api_key)

    def to_task_client(self) -> DatashareTaskClient:
        return self.datashare.to_task_client()

    async def to_temporal_client(self) -> TemporalClient:
        return await self.temporal.to_client()
