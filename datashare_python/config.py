from typing import ClassVar

from icij_common.pydantic_utils import ICIJSettings, NoEnumModel
from icij_worker.utils.logging_ import LogWithWorkerIDMixin
from pydantic import Field

import datashare_python

_ALL_LOGGERS = [datashare_python.__name__]


class AppConfig(ICIJSettings, LogWithWorkerIDMixin, NoEnumModel):
    class Config:
        env_prefix = "DS_DOCKER_ML_"

    loggers: ClassVar[list[str]] = Field(_ALL_LOGGERS, const=True)

    log_level: str = Field(default="INFO")

    batch_size: int = 1024
    pipeline_batch_size: int = 1024
    ne_buffer_size: int = 1000

    # DS
    ds_api_key: str | None = None
    ds_url: str = "http://datashare:8080"
    # ES
    es_address: str = "http://localhost:9200"
    es_default_page_size: int = 1000
    es_keep_alive: str = "10m"
    es_max_concurrency: int = 5
    es_max_retries: int = 0
    es_max_retry_wait_s: int | float = 60
    es_timeout_s: int | float = 60 * 5

    def to_es_client(self, address: str | None = None) -> "ESClient":
        from icij_common.es import ESClient

        if address is None:
            address = self.es_address

        client = ESClient(
            hosts=[address],
            pagination=self.es_default_page_size,
            max_concurrency=self.es_max_concurrency,
            keep_alive=self.es_keep_alive,
            timeout=self.es_timeout_s,
            max_retries=self.es_max_retries,
            max_retry_wait_s=self.es_max_retry_wait_s,
            api_key=self.ds_api_key,
        )
        client.transport._verified_elasticsearch = (  # pylint: disable=protected-access
            True
        )
        return client

    def to_task_client(self) -> "DatashareTaskClient":
        from datashare_python.task_client import DatashareTaskClient

        return DatashareTaskClient(self.ds_url)
