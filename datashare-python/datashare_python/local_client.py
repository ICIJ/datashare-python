import asyncio
import importlib
import logging
from functools import partial

from google.protobuf.duration_pb2 import Duration
from temporalio.api.workflowservice.v1 import (
    ListNamespacesRequest,
    RegisterNamespaceRequest,
)
from temporalio.service import ConnectConfig, ServiceClient

LOGGER = logging.getLogger(__name__)


class LocalClient:
    @staticmethod
    async def start_workers(
        temporal_address: str, worker_paths: list[str], *args, **kwargs
    ) -> None:
        """Start worker modules, defined as a list of worker paths

        :param temporal_address: temporal address string (host:port)
        :param worker_paths: list of worker modules
        """
        workers = []

        for worker_path in worker_paths:
            try:
                module_parts = worker_path.split(".")
                module_path = ".".join(module_parts[:-1])
                worker_method = module_parts[-1]
                module = importlib.import_module(module_path)
                worker = getattr(module, worker_method)
                worker_partial = partial(worker, *args, **kwargs)

                workers.append(worker_partial(target_host=temporal_address))

                LOGGER.info("'%s' imported successfully.", worker_path)
            except (ModuleNotFoundError, AttributeError):
                LOGGER.error("'%s' not found in path. Skipping.", worker_path)
                continue

        await asyncio.gather(*workers, return_exceptions=True)

    @staticmethod
    async def register_namespace(temporal_address: str, namespace: str) -> None:
        """Register a temporal namespace

        :param temporal_address: temporal address string
        :param namespace: namespace string
        """
        client = await ServiceClient.connect(
            ConnectConfig(target_host=temporal_address)
        )
        list_resp = await client.workflow_service.list_namespaces(
            ListNamespacesRequest()
        )

        if namespace in [ns.namespace_info.name for ns in list_resp.namespaces]:
            return

        await client.workflow_service.register_namespace(
            RegisterNamespaceRequest(
                namespace=namespace,
                # retain for thirty days
                workflow_execution_retention_period=Duration(seconds=30 * 24 * 60 * 60),
            )
        )
