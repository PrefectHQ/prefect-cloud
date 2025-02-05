from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from httpx import AsyncClient, Response


HTTP_METHODS: TypeAlias = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]


ServerRoutes = Literal[
    "/block_capabilities/",
    "/block_documents/",
    "/block_documents/{id}",
    "/block_documents/count",
    "/block_documents/filter",
    "/block_schemas/",
    "/block_schemas/{id}",
    "/block_schemas/checksum/{checksum}",
    "/block_schemas/filter",
    "/block_types/",
    "/block_types/{id}",
    "/block_types/filter",
    "/block_types/install_system_block_types",
    "/block_types/slug/{slug}",
    "/block_types/slug/{slug}/block_documents",
    "/block_types/slug/{slug}/block_documents/name/{block_document_name}",
    "/collections/views/{view}",
    "/concurrency_limits/",
    "/concurrency_limits/{id}",
    "/concurrency_limits/decrement",
    "/concurrency_limits/filter",
    "/concurrency_limits/increment",
    "/concurrency_limits/tag/{tag}",
    "/concurrency_limits/tag/{tag}/reset",
    "/csrf-token",
    "/deployments/",
    "/deployments/{id}",
    "/deployments/{id}/create_flow_run",
    "/deployments/{id}/pause_deployment",
    "/deployments/{id}/resume_deployment",
    "/deployments/{id}/schedule",
    "/deployments/{id}/schedules",
    "/deployments/{id}/schedules/{schedule_id}",
    "/deployments/{id}/work_queue_check",
    "/deployments/count",
    "/deployments/filter",
    "/deployments/get_scheduled_flow_runs",
    "/deployments/name/{flow_name}/{deployment_name}",
    "/deployments/paginate",
    "/flows/",
    "/flows/{id}",
    "/flows/count",
    "/flows/filter",
    "/flows/name/{name}",
    "/flows/paginate",
    "/health",
    "/ui/task_runs/dashboard/counts",
    "/v2/concurrency_limits/",
    "/v2/concurrency_limits/{id_or_name}",
    "/v2/concurrency_limits/decrement",
    "/v2/concurrency_limits/filter",
    "/v2/concurrency_limits/increment",
    "/work_pools/",
    "/work_pools/{name}",
    "/work_pools/{name}/get_scheduled_flow_runs",
    "/work_pools/{work_pool_name}/queues",
    "/work_pools/{work_pool_name}/queues/{name}",
    "/work_pools/{work_pool_name}/queues/filter",
    "/work_pools/{work_pool_name}/workers/{name}",
    "/work_pools/{work_pool_name}/workers/filter",
    "/work_pools/{work_pool_name}/workers/heartbeat",
    "/work_pools/count",
    "/work_pools/filter",
    "/work_queues/",
    "/work_queues/{id}",
    "/work_queues/{id}/get_runs",
    "/work_queues/{id}/status",
    "/work_queues/filter",
    "/work_queues/name/{name}",
]


class BaseAsyncClient:
    def __init__(self, client: "AsyncClient"):
        self._client = client

    async def request(
        self,
        method: HTTP_METHODS,
        path: "ServerRoutes",
        params: dict[str, Any] | None = None,
        path_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> "Response":
        if path_params:
            path = path.format(**path_params)  # type: ignore
        return await self._client.request(method, path, params=params, **kwargs)
