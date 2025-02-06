from __future__ import annotations
from typing import Any, Literal, NoReturn, Optional
from typing_extensions import TypeAlias, Self
from contextlib import AsyncExitStack
import httpcore
import httpx
import asyncio
from prefect_cloud.schemas.actions import (
    WorkPoolCreate,
    BlockDocumentCreate,
    BlockDocumentUpdate,
)
from prefect_cloud.schemas.filters import WorkPoolFilter, WorkPoolFilterType
from prefect_cloud.utilities.exception import ObjectNotFound

from prefect.utilities.callables import ParameterSchema
from prefect.workers.utilities import (
    get_default_base_job_template_for_infrastructure_type,
)
from prefect_cloud.settings import settings
from prefect_cloud.clients.work_pools import WorkPoolAsyncClient
from prefect_cloud.clients.deployments import DeploymentAsyncClient
from prefect_cloud.clients.flow import FlowAsyncClient
from prefect_cloud.clients.blocks import BlocksDocumentAsyncClient
from prefect_cloud.utilities.collections import AutoEnum

PREFECT_MANAGED = "prefect:managed"


HTTP_METHODS: TypeAlias = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]

PREFECT_API_REQUEST_TIMEOUT = 60.0


class ServerType(AutoEnum):
    EPHEMERAL = AutoEnum.auto()
    SERVER = AutoEnum.auto()
    CLOUD = AutoEnum.auto()
    UNCONFIGURED = AutoEnum.auto()


class PrefectCloudClient(
    WorkPoolAsyncClient,
    DeploymentAsyncClient,
    FlowAsyncClient,
    BlocksDocumentAsyncClient,
):
    def __init__(self, api_url: str, api_key: str):
        httpx_settings: dict[str, Any] = {}
        httpx_settings.setdefault("headers", {"Authorization": f"Bearer {api_key}"})
        httpx_settings.setdefault("base_url", api_url)

        self._context_stack: int = 0
        self._exit_stack = AsyncExitStack()

        self._closed = False
        self._started = False

        # See https://www.python-httpx.org/advanced/#pool-limit-configuration
        httpx_settings.setdefault(
            "limits",
            httpx.Limits(
                # We see instability when allowing the client to open many connections at once.
                # Limiting concurrency results in more stable performance.
                max_connections=16,
                max_keepalive_connections=8,
                # The Prefect Cloud LB will keep connections alive for 30s.
                # Only allow the client to keep connections alive for 25s.
                keepalive_expiry=25,
            ),
        )

        # See https://www.python-httpx.org/http2/
        # Enabling HTTP/2 support on the client does not necessarily mean that your requests
        # and responses will be transported over HTTP/2, since both the client and the server
        # need to support HTTP/2. If you connect to a server that only supports HTTP/1.1 the
        # client will use a standard HTTP/1.1 connection instead.
        httpx_settings.setdefault("http2", False)
        httpx_settings.setdefault(
            "timeout",
            httpx.Timeout(
                connect=PREFECT_API_REQUEST_TIMEOUT,
                read=PREFECT_API_REQUEST_TIMEOUT,
                write=PREFECT_API_REQUEST_TIMEOUT,
                pool=PREFECT_API_REQUEST_TIMEOUT,
            ),
        )
        self._client = httpx.AsyncClient(**httpx_settings)
        self._loop = None

        # See https://www.python-httpx.org/advanced/#custom-transports
        #
        # If we're using an HTTP/S client (not the ephemeral client), adjust the
        # transport to adds retries _after_ it is instantiated. If we alter the transport
        # before instantiation, the transport will not be aware of proxies unless we
        # reproduce all of the logic to make it so.
        #
        # Only alter the transport to set our default of 3 retries, don't modify any
        # transport a user may have provided via httpx_settings.
        #
        # Making liberal use of getattr and isinstance checks here to avoid any
        # surprises if the internals of httpx or httpcore change on us
        if not httpx_settings.get("transport"):
            transport_for_url = getattr(self._client, "_transport_for_url", None)
            if callable(transport_for_url):
                server_transport = transport_for_url(httpx.URL(api_url))
                if isinstance(server_transport, httpx.AsyncHTTPTransport):
                    pool = getattr(server_transport, "_pool", None)
                    if isinstance(pool, httpcore.AsyncConnectionPool):
                        setattr(pool, "_retries", 3)

    async def ensure_managed_work_pool(
        self, name: str = settings.default_managed_work_pool_name
    ) -> str:
        work_pools = await self.read_work_pools(
            work_pool_filter=WorkPoolFilter(
                type=WorkPoolFilterType(any_=[PREFECT_MANAGED])
            )
        )

        if work_pools:
            return work_pools[0].name

        template = await get_default_base_job_template_for_infrastructure_type(
            PREFECT_MANAGED
        )
        if template is None:
            raise ValueError("No default base job template found for managed work pool")

        work_pool = await self.create_work_pool(
            work_pool=WorkPoolCreate(
                name=name,
                type=PREFECT_MANAGED,
                base_job_template=template,
            ),
            overwrite=True,
        )

        return work_pool.name

    async def create_managed_deployment(
        self,
        deployment_name: str,
        filename: str,
        flow_func: str,
        work_pool_name: str,
        pull_steps: list[dict[str, Any]],
        parameter_schema: ParameterSchema,
        job_variables: dict[str, Any] | None = None,
    ):
        flow_id = await self.create_flow_from_name(flow_func)

        deployment_id = await self.create_deployment(
            flow_id=flow_id,
            entrypoint=f"{filename}:{flow_func}",
            name=deployment_name,
            work_pool_name=work_pool_name,
            pull_steps=pull_steps,
            parameter_openapi_schema=parameter_schema.model_dump_for_openapi(),
            job_variables=job_variables,
        )

        return deployment_id

    async def create_credentials_secret(self, name: str, credentials: str):
        try:
            existing_block = await self.read_block_document_by_name(
                name, block_type_slug="secret"
            )
            await self.update_block_document(
                block_document_id=existing_block.id,
                block_document=BlockDocumentUpdate(
                    data={
                        "value": credentials,
                    },
                ),
            )
        except ObjectNotFound:
            secret_block_type = await self.read_block_type_by_slug("secret")
            secret_block_schema = (
                await self.get_most_recent_block_schema_for_block_type(
                    block_type_id=secret_block_type.id
                )
            )
            await self.create_block_document(
                block_document=BlockDocumentCreate(
                    name=name,
                    data={
                        "value": credentials,
                    },
                    block_type_id=secret_block_type.id,
                    block_schema_id=secret_block_schema.id,
                )
            )

    async def __aenter__(self) -> Self:
        """
        Start the client.

        If the client is already started, this will raise an exception.

        If the client is already closed, this will raise an exception. Use a new client
        instance instead.
        """
        if self._closed:
            # httpx.AsyncClient does not allow reuse so we will not either.
            raise RuntimeError(
                "The client cannot be started again after closing. "
                "Retrieve a new client with `get_client()` instead."
            )

        self._context_stack += 1

        if self._started:
            # allow reentrancy
            return self

        self._loop = asyncio.get_running_loop()
        await self._exit_stack.__aenter__()

        # Enter a lifespan context if using an ephemeral application.
        # See https://github.com/encode/httpx/issues/350

        # Enter the httpx client's context
        await self._exit_stack.enter_async_context(self._client)

        self._started = True

        return self

    async def __aexit__(self, *exc_info: Any) -> Optional[bool]:
        """
        Shutdown the client.
        """

        self._context_stack -= 1
        if self._context_stack > 0:
            return
        self._closed = True
        return await self._exit_stack.__aexit__(*exc_info)

    def __enter__(self) -> NoReturn:
        raise RuntimeError(
            "The `PrefectClient` must be entered with an async context. Use 'async "
            "with PrefectClient(...)' not 'with PrefectClient(...)'"
        )

    def __exit__(self, *_: object) -> NoReturn:
        assert False, "This should never be called but must be defined for __enter__"


def get_prefect_cloud_client(api_url: str, api_key: str):
    return PrefectCloudClient(
        api_url=api_url,
        api_key=api_key,
    )
