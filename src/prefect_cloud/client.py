from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Union
from uuid import UUID

import httpx
from httpx import HTTPStatusError, RequestError
from typing_extensions import TypeAlias

from prefect_cloud.schemas.actions import (
    BlockDocumentCreate,
)
from prefect_cloud.schemas.objects import (
    BlockDocument,
    BlockSchema,
    BlockType,
    CronSchedule,
    DeploymentFlowRun,
    DeploymentSchedule,
    Flow,
    WorkPool,
)
from prefect_cloud.schemas.responses import DeploymentResponse
from prefect_cloud.settings import settings
from prefect_cloud.utilities.blocks import safe_block_name
from prefect_cloud.utilities.callables import ParameterSchema
from prefect_cloud.utilities.exception import ObjectAlreadyExists, ObjectNotFound
from prefect_cloud.utilities.generics import validate_list

if TYPE_CHECKING:
    from prefect_cloud.schemas.objects import Deployment

PREFECT_MANAGED = "prefect:managed"
HTTP_METHODS: TypeAlias = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]

PREFECT_API_REQUEST_TIMEOUT = 60.0


async def _retry_request(
    request_func: Any,
    *args: Any,
    max_retries: int = 3,
    base_delay: float = 1.0,
    **kwargs: Any,
) -> httpx.Response:
    """
    Execute an async request function with exponential backoff retry logic.

    Retries on:
    - ReadTimeout, ConnectTimeout, NetworkError (transient network issues)
    - 5xx server errors
    - 429 rate limit errors

    Does not retry on:
    - 4xx client errors (except 429)
    """
    for attempt in range(max_retries + 1):
        try:
            response = await request_func(*args, **kwargs)

            # Check if response has retryable status code
            if response.status_code >= 500 or response.status_code == 429:
                if attempt < max_retries:
                    # Wait with exponential backoff before retry
                    delay = base_delay * (2**attempt)
                    jitter = random.uniform(0.1, 0.3) * delay
                    await asyncio.sleep(delay + jitter)
                    continue
                # On last attempt with retryable status, return the response
                # Let the caller decide how to handle the error status
                return response

            return response

        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.NetworkError) as exc:
            if attempt < max_retries:
                # Wait with exponential backoff before retry
                delay = base_delay * (2**attempt)
                jitter = random.uniform(0.1, 0.3) * delay
                await asyncio.sleep(delay + jitter)
                continue
            # On last attempt, re-raise the exception
            raise exc
        except Exception as exc:
            # Don't retry other exceptions (e.g., HTTPStatusError for 4xx)
            raise exc

    # This should never be reached with valid parameters, but satisfy type checker
    raise RuntimeError(
        f"Retry loop completed unexpectedly with max_retries={max_retries}"
    )


class PrefectCloudClient(httpx.AsyncClient):
    def __init__(self, api_url: str, api_key: str):
        httpx_settings: dict[str, Any] = {}
        httpx_settings.setdefault("headers", {"Authorization": f"Bearer {api_key}"})
        httpx_settings.setdefault("base_url", api_url)
        super().__init__(**httpx_settings)

    async def request(
        self,
        method: str | HTTP_METHODS,
        url: httpx.URL | str,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Make an HTTP request with retry logic for transient errors.

        This method wraps the parent request method with exponential backoff
        retry logic to handle network timeouts and server errors.
        """
        return await _retry_request(super().request, method, url, **kwargs)

    @property
    def account_url(self) -> str:
        """
        Extract the account-level URL from the workspace URL.

        For URLs like "https://api.prefect.cloud/api/accounts/123/workspaces/456",
        returns "https://api.prefect.cloud/api/accounts/123"
        """
        base_url = str(self.base_url)
        if "workspaces/" in base_url:
            return base_url.split("workspaces/")[0].rstrip("/")
        return base_url

    async def account_request(
        self, method: HTTP_METHODS, path: str, **kwargs: Any
    ) -> httpx.Response:
        """
        Make a request to an account-level endpoint.

        Args:
            method: HTTP method to use
            path: Path to append to the account URL
            **kwargs: Additional arguments to pass to the request

        Returns:
            The HTTP response
        """
        url = f"{self.account_url}/{path.lstrip('/')}"
        return await self.request(method, url, **kwargs)

    async def get_github_state_token(self, redirect_url: str | None = None) -> str:
        """
        Get a state token for GitHub integration.

        Returns:
            The state token
        """
        response = await self.account_request(
            "POST", "integrations/github/state-token", json=redirect_url
        )
        response.raise_for_status()
        return response.json()["state_token"]

    async def get_github_token(
        self,
        owner: str,
        repository: str,
    ) -> str | None:
        """
        Get a GitHub token for a specific repository.

        Args:
            owner: The GitHub repository owner
            repository: The GitHub repository name

        Returns:
            The GitHub token or None
        """
        try:
            response = await self.account_request(
                "POST",
                "integrations/github/token",
                json={
                    "owner": owner,
                    "repository": repository,
                },
            )
            response.raise_for_status()
        except HTTPStatusError:
            return None

        return response.json()["token"]

    async def get_github_repositories(self) -> list[str]:
        """
        Get a list of GitHub repositories that the account has access to.

        Returns:
            A list of repository names
        """
        response = await self.account_request(
            "GET",
            "integrations/github/repositories",
        )
        try:
            response.raise_for_status()
        except HTTPStatusError:
            return []

        return response.json()["repositories"]

    async def read_managed_work_pools(
        self,
    ) -> list["WorkPool"]:
        """
        Reads work pools.

        Args:
            limit: Limit for the work pool query.
            offset: Offset for the work pool query.

        Returns:
            A list of work pools.
        """
        from prefect_cloud.schemas.objects import WorkPool

        body: dict[str, Any] = {
            "limit": None,
            "offset": 0,
            "work_pools": {"type": {"any_": [PREFECT_MANAGED]}},
        }
        response = await self.request("POST", "/work_pools/filter", json=body)
        return validate_list(WorkPool, response.json())

    async def read_work_pool_by_name(self, name: str) -> "WorkPool":
        response = await self.request("GET", f"/work_pools/{name}")
        return WorkPool.model_validate(response.json())

    async def create_work_pool_managed_by_name(
        self,
        name: str,
        template: dict[str, Any],
    ) -> "WorkPool":
        """
        Creates a work pool with the provided configuration.

        Args:
            work_pool: Desired configuration for the new work pool.

        Returns:
            Information about the newly created work pool.
        """
        from prefect_cloud.schemas.objects import WorkPool

        try:
            response = await self.request(
                "POST",
                "/work_pools/",
                json={
                    "name": name,
                    "type": PREFECT_MANAGED,
                    "base_job_template": template,
                },
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            if e.response.status_code == 409:
                raise ObjectAlreadyExists(http_exc=e) from e
            else:
                raise

        return WorkPool.model_validate(response.json())

    async def create_flow_from_name(self, flow_name: str) -> "UUID":
        """
        Create a flow in the Prefect API.

        Args:
            flow_name: the name of the new flow

        Raises:
            httpx.RequestError: if a flow was not created for any reason

        Returns:
            the ID of the flow in the backend
        """

        flow_data = {"name": flow_name}
        response = await self.request("POST", "/flows/", json=flow_data)

        flow_id = response.json().get("id")
        if not flow_id:
            raise RequestError(f"Malformed response: {response}")

        # Return the id of the created flow
        from uuid import UUID

        return UUID(flow_id)

    async def create_deployment(
        self,
        flow_id: "UUID",
        name: str,
        entrypoint: str,
        work_pool_name: str,
        pull_steps: list[dict[str, Any]] | None = None,
        parameter_openapi_schema: dict[str, Any] | None = None,
        job_variables: dict[str, Any] | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> "UUID":
        """
        Create a deployment.

        Args:
            flow_id: the flow ID to create a deployment for
            name: the name of the deployment
            entrypoint: the entrypoint path for the flow
            work_pool_name: the name of the work pool to use
            pull_steps: steps to pull code/data before running the flow
            parameter_openapi_schema: OpenAPI schema for flow parameters
            job_variables: A dictionary of dot delimited infrastructure overrides that
                will be applied at runtime
            parameters: Default parameter values to pass to the flow at runtime
        Returns:
            the ID of the deployment in the backend
        """
        from prefect_cloud.schemas.actions import DeploymentCreate

        if parameter_openapi_schema is None:
            parameter_openapi_schema = {}

        deployment_create = DeploymentCreate(
            flow_id=flow_id,
            name=name,
            entrypoint=entrypoint,
            work_pool_name=work_pool_name,
            pull_steps=pull_steps,
            parameter_openapi_schema=parameter_openapi_schema,
            job_variables=dict(job_variables or {}),
            parameters=parameters or {},
        )

        json = deployment_create.model_dump(mode="json")
        response = await self.request(
            "POST",
            "/deployments/",
            json=json,
        )
        deployment_id = response.json().get("id")
        if not deployment_id:
            raise RequestError(f"Malformed response: {response}")

        return UUID(deployment_id)

    async def delete_deployment(self, deployment_id: "UUID"):
        try:
            await self.request("DELETE", f"/deployments/{deployment_id}")
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def upsert_block_document(
        self,
        block_document: "BlockDocument | BlockDocumentCreate",
        include_secrets: bool = True,
    ) -> "BlockDocument":
        block_document_data = block_document.model_dump(
            mode="json",
            exclude_unset=True,
            context={"include_secrets": include_secrets},
            serialize_as_any=True,
        )
        try:
            response = await self.request(
                "PUT",
                "/block_documents/",
                json=block_document_data,
            )
            response.raise_for_status()
        except HTTPStatusError:
            raise
        from prefect_cloud.schemas.objects import BlockDocument

        return BlockDocument.model_validate(response.json())

    async def read_block_type_by_slug(self, slug: str) -> "BlockType":
        """
        Read a block type by its slug.
        """
        try:
            response = await self.request(
                "GET",
                f"/block_types/slug/{slug}",
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise
        from prefect_cloud.schemas.objects import BlockType

        return BlockType.model_validate(response.json())

    async def get_most_recent_block_schema_for_block_type(
        self,
        block_type_id: "UUID",
    ) -> BlockSchema | None:
        """
        Fetches the most recent block schema for a specified block type ID.

        Args:
            block_type_id: The ID of the block type.

        Raises:
            httpx.RequestError: If the request fails for any reason.

        Returns:
            The most recent block schema or None.
        """
        try:
            response = await self.request(
                "POST",
                "/block_schemas/filter",
                json={
                    "block_schemas": {"block_type_id": {"any_": [str(block_type_id)]}},
                    "limit": 1,
                },
            )
            response.raise_for_status()
        except HTTPStatusError:
            raise

        return next(iter(validate_list(BlockSchema, response.json())), None)

    async def read_deployment(
        self,
        deployment_id: Union["UUID", str],
    ) -> "DeploymentResponse":
        """
        Query the Prefect API for a deployment by id.

        Args:
            deployment_id: the deployment ID of interest

        Returns:
            a [Deployment model][prefect.client.schemas.objects.Deployment] representation of the deployment
        """
        from uuid import UUID

        from prefect_cloud.schemas.responses import DeploymentResponse

        if not isinstance(deployment_id, UUID):
            try:
                deployment_id = UUID(deployment_id)
            except ValueError:
                raise ValueError(f"Invalid deployment ID: {deployment_id}")

        try:
            response = await self.request(
                "GET",
                f"/deployments/{deployment_id}",
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

        return DeploymentResponse.model_validate(response.json())

    async def read_deployment_by_name(
        self,
        name: str,
    ) -> "DeploymentResponse":
        """
        Query the Prefect API for a deployment by name.

        Args:
            name: A deployed flow's name: <FLOW_NAME>/<DEPLOYMENT_NAME>

        Raises:
            ObjectNotFound: If request returns 404
            RequestError: If request fails

        Returns:
            a Deployment model representation of the deployment
        """
        from prefect_cloud.schemas.responses import DeploymentResponse

        try:
            flow_name, deployment_name = name.split("/")
            response = await self.request(
                "GET",
                f"/deployments/name/{flow_name}/{deployment_name}",
            )
            response.raise_for_status()
        except (HTTPStatusError, ValueError) as e:
            if isinstance(e, HTTPStatusError) and e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            elif isinstance(e, ValueError):
                raise ValueError(
                    f"Invalid deployment name format: {name}. Expected format: <FLOW_NAME>/<DEPLOYMENT_NAME>"
                ) from e
            else:
                raise

        return DeploymentResponse.model_validate(response.json())

    async def read_all_flows(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list["Flow"]:
        """
        Query the Prefect API for flows. Only flows matching all criteria will
        be returned.

        Args:
            sort: sort criteria for the flows
            limit: limit for the flow query
            offset: offset for the flow query

        Returns:
            a list of Flow model representations of the flows
        """
        body: dict[str, Any] = {
            "sort": None,
            "limit": limit,
            "offset": offset,
        }

        response = await self.request("POST", "/flows/filter", json=body)
        from prefect_cloud.schemas.objects import Flow

        return validate_list(Flow, response.json())

    async def read_all_deployments(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list["Deployment"]:
        """
        Query the Prefect API for deployments. Only deployments matching all
        the provided criteria will be returned.

        Args:
            limit: a limit for the deployment query
            offset: an offset for the deployment query

        Returns:
            a list of Deployment model representations
                of the deployments
        """
        from prefect_cloud.schemas.objects import Deployment

        body: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "sort": None,
        }

        response = await self.request("POST", "/deployments/filter", json=body)
        return validate_list(Deployment, response.json())

    async def create_deployment_schedule(
        self,
        deployment_id: "UUID",
        schedule: CronSchedule,
        active: bool,
        parameters: dict[str, Any] | None = None,
    ) -> "DeploymentSchedule":
        """
        Create deployment schedules.

        Args:
            deployment_id: the deployment ID
            schedules: a list of tuples containing the schedule to create
                       and whether or not it should be active.

        Raises:
            RequestError: if the schedules were not created for any reason

        Returns:
            the list of schedules created in the backend
        """
        from prefect_cloud.schemas.actions import DeploymentScheduleCreate
        from prefect_cloud.schemas.objects import DeploymentSchedule

        json = DeploymentScheduleCreate(
            schedule=schedule,
            active=active,
            parameters=parameters,
        ).model_dump(mode="json")

        response = await self.request(
            "POST",
            f"/deployments/{deployment_id}/schedules",
            json=[json],
        )
        return validate_list(DeploymentSchedule, response.json())[0]

    async def read_deployment_schedules(
        self,
        deployment_id: "UUID",
    ) -> list["DeploymentSchedule"]:
        """
        Query the Prefect API for a deployment's schedules.

        Args:
            deployment_id: the deployment ID

        Returns:
            a list of DeploymentSchedule model representations of the deployment schedules
        """
        from prefect_cloud.schemas.objects import DeploymentSchedule

        try:
            response = await self.request(
                "GET",
                f"/deployments/{deployment_id}/schedules",
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise
        return validate_list(DeploymentSchedule, response.json())

    async def update_deployment_schedule_active(
        self,
        deployment_id: "UUID",
        schedule_id: "UUID",
        active: bool | None = None,
    ) -> None:
        """
        Update a deployment schedule by ID.

        Args:
            deployment_id: the deployment ID
            schedule_id: the deployment schedule ID of interest
            active: whether or not the schedule should be active
        """
        try:
            response = await self.request(
                "PATCH",
                f"/deployments/{deployment_id}/schedules/{schedule_id}",
                json={"active": active},
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def delete_deployment_schedule(
        self,
        deployment_id: "UUID",
        schedule_id: "UUID",
    ) -> None:
        """
        Delete a deployment schedule.

        Args:
            deployment_id: the deployment ID
            schedule_id: the ID of the deployment schedule to delete.

        Raises:
            RequestError: if the schedules were not deleted for any reason
        """
        try:
            response = await self.request(
                "DELETE",
                f"/deployments/{deployment_id}/schedules/{schedule_id}",
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def create_flow_run_from_deployment_id(
        self,
        deployment_id: "UUID",
        parameters: dict[str, Any] | None = None,
    ) -> "DeploymentFlowRun":
        """
        Create a flow run for a deployment.

        Args:
            deployment_id: The deployment ID to create the flow run from

        Raises:
            RequestError: if the Prefect API does not successfully create a run for any reason

        Returns:
            The flow run model
        """
        from prefect_cloud.schemas.objects import DeploymentFlowRun

        response = await self.request(
            "POST",
            f"/deployments/{deployment_id}/create_flow_run",
            json={"parameters": parameters or {}},
        )
        return DeploymentFlowRun.model_validate(response.json())

    async def read_next_scheduled_flow_runs_by_deployment_ids(
        self,
        deployment_ids: list[UUID],
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> "list[DeploymentFlowRun]":
        """
        Query the Prefect API for flow runs. Only flow runs matching all criteria will
        be returned.

        Args:

            sort: sort criteria for the flow runs
            limit: limit for the flow run query
            offset: offset for the flow run query

        Returns:
            a list of Flow Run model representations
                of the flow runs
        """

        body: dict[str, Any] = {
            "deployment_id": {"any_": [str(id) for id in deployment_ids]},
            "state": {"any_": ["SCHEDULED"]},
            "expected_start_time": {"after_": datetime.now(timezone.utc).isoformat()},
            "sort": "EXPECTED_START_TIME_ASC",
            "limit": limit,
            "offset": offset,
        }

        response = await self.request("POST", "/flow_runs/filter", json=body)

        from prefect_cloud.schemas.objects import DeploymentFlowRun

        return validate_list(DeploymentFlowRun, response.json())

    async def get_default_managed_work_pool(self) -> WorkPool | None:
        work_pools = await self.read_managed_work_pools()
        if work_pools:
            return work_pools[0]

    async def ensure_managed_work_pool(
        self, name: str = settings.default_managed_work_pool_name
    ) -> WorkPool:
        work_pool = await self.get_default_managed_work_pool()
        if work_pool:
            return work_pool

        template = await self.get_default_base_job_template_for_managed_work_pool()
        if template is None:
            raise ValueError("No default base job template found for managed work pool")

        work_pool = await self.create_work_pool_managed_by_name(
            name=name,
            template=template,
        )

        return work_pool

    async def create_managed_deployment(
        self,
        deployment_name: str,
        filepath: str,
        function: str,
        work_pool_name: str,
        pull_steps: list[dict[str, Any]],
        parameter_schema: ParameterSchema,
        job_variables: dict[str, Any] | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> UUID:
        flow_id = await self.create_flow_from_name(function)

        deployment_id = await self.create_deployment(
            flow_id=flow_id,
            entrypoint=f"{filepath}:{function}",
            name=deployment_name,
            work_pool_name=work_pool_name,
            pull_steps=pull_steps,
            parameter_openapi_schema=parameter_schema.model_dump_for_openapi(),
            job_variables=job_variables,
            parameters=parameters,
        )

        return deployment_id

    async def create_or_replace_secret(self, name: str, secret: str) -> str:
        try:
            safe_name = safe_block_name(name)
            secret_block_type = await self.read_block_type_by_slug("secret")
            secret_block_schema = (
                await self.get_most_recent_block_schema_for_block_type(
                    block_type_id=secret_block_type.id
                )
            )
            if secret_block_schema is None:
                raise ValueError("No secret block schema found")

            block = await self.upsert_block_document(
                BlockDocumentCreate(
                    name=safe_name,
                    data={"value": secret},
                    block_type_id=secret_block_type.id,
                    block_schema_id=secret_block_schema.id,
                )
            )
            assert block.name
            return block.name
        except HTTPStatusError:
            raise

    async def get_default_base_job_template_for_managed_work_pool(
        self,
    ) -> Optional[Dict[str, Any]]:
        try:
            response = await self.request("GET", "collections/work_pool_types")
            worker_metadata = response.json()
            for collection in worker_metadata.values():
                for worker in collection.values():
                    if worker.get("type") == PREFECT_MANAGED:
                        return worker.get("default_base_job_configuration")
        except Exception:
            pass
        return None


class SyncPrefectCloudClient(httpx.Client):
    def __init__(self, api_url: str, api_key: str):
        httpx_settings: dict[str, Any] = {}
        httpx_settings.setdefault("headers", {"Authorization": f"Bearer {api_key}"})
        httpx_settings.setdefault("base_url", api_url)
        super().__init__(**httpx_settings)
