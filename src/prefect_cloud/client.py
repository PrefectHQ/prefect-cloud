from __future__ import annotations

from datetime import datetime, timezone
from logging import getLogger
from typing import Any, Dict, Literal, Optional, Union
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
from prefect_cloud.utilities.callables import ParameterSchema
from prefect_cloud.utilities.exception import ObjectAlreadyExists, ObjectNotFound
from prefect_cloud.utilities.generics import validate_list

PREFECT_MANAGED = "prefect:managed"
HTTP_METHODS: TypeAlias = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]

PREFECT_API_REQUEST_TIMEOUT = 60.0
logger = getLogger(__name__)


class PrefectCloudClient(httpx.AsyncClient):
    def __init__(self, api_url: str, api_key: str):
        httpx_settings: dict[str, Any] = {}
        httpx_settings.setdefault("headers", {"Authorization": f"Bearer {api_key}"})
        httpx_settings.setdefault("base_url", api_url)
        super().__init__(**httpx_settings)

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

    async def create_block_document(
        self,
        block_document: "BlockDocument | BlockDocumentCreate",
        include_secrets: bool = True,
    ) -> "BlockDocument":
        """
        Create a block document in the Prefect API. This data is used to configure a
        corresponding Block.

        Args:
            include_secrets (bool): whether to include secret values
                on the stored Block, corresponding to Pydantic's `SecretStr` and
                `SecretBytes` fields. Note Blocks may not work as expected if
                this is set to `False`.
        """
        block_document_data = block_document.model_dump(
            mode="json",
            exclude_unset=True,
            exclude={"id", "block_schema", "block_type"},
            context={"include_secrets": include_secrets},
            serialize_as_any=True,
        )
        try:
            response = await self.request(
                "POST",
                "/block_documents/",
                json=block_document_data,
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            if e.response.status_code == 409:
                raise ObjectAlreadyExists(http_exc=e) from e
            else:
                raise
        from prefect_cloud.schemas.objects import BlockDocument

        return BlockDocument.model_validate(response.json())

    async def update_block_document_value(
        self,
        block_document_id: "UUID",
        value: str,
    ) -> None:
        """
        Update a block document in the Prefect API.
        """
        try:
            await self.request(
                "PATCH",
                f"/block_documents/{block_document_id}",
                json={"data": {"value": value}},
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def read_block_document(
        self,
        block_document_id: "UUID",
        include_secrets: bool = True,
    ) -> "BlockDocument":
        """
        Read the block document with the specified ID.

        Args:
            block_document_id: the block document id
            include_secrets (bool): whether to include secret values
                on the Block, corresponding to Pydantic's `SecretStr` and
                `SecretBytes` fields. These fields are automatically obfuscated
                by Pydantic, but users can additionally choose not to receive
                their values from the API. Note that any business logic on the
                Block may not work if this is `False`.

        Raises:
            httpx.RequestError: if the block document was not found for any reason

        Returns:
            A block document or None.
        """
        try:
            response = await self.request(
                "GET",
                f"/block_documents/{block_document_id}",
                params=dict(include_secrets=include_secrets),
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise
        from prefect_cloud.schemas.objects import BlockDocument

        return BlockDocument.model_validate(response.json())

    async def read_block_documents(
        self,
        block_schema_type: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
        include_secrets: bool = True,
    ) -> "list[BlockDocument]":
        """
        Read block documents

        Args:
            block_schema_type: an optional block schema type
            offset: an offset
            limit: the number of blocks to return
            include_secrets (bool): whether to include secret values
                on the Block, corresponding to Pydantic's `SecretStr` and
                `SecretBytes` fields. These fields are automatically obfuscated
                by Pydantic, but users can additionally choose not to receive
                their values from the API. Note that any business logic on the
                Block may not work if this is `False`.

        Returns:
            A list of block documents
        """
        response = await self.request(
            "POST",
            "/block_documents/filter",
            json=dict(
                block_schema_type=block_schema_type,
                offset=offset,
                limit=limit,
                include_secrets=include_secrets,
            ),
        )
        from prefect_cloud.schemas.objects import BlockDocument

        return validate_list(BlockDocument, response.json())

    async def read_block_document_by_name(
        self,
        name: str,
        block_type_slug: str,
        include_secrets: bool = True,
    ) -> "BlockDocument":
        """
        Read the block document with the specified name that corresponds to a
        specific block type name.

        Args:
            name: The block document name.
            block_type_slug: The block type slug.
            include_secrets (bool): whether to include secret values
                on the Block, corresponding to Pydantic's `SecretStr` and
                `SecretBytes` fields. These fields are automatically obfuscated
                by Pydantic, but users can additionally choose not to receive
                their values from the API. Note that any business logic on the
                Block may not work if this is `False`.

        Raises:
            httpx.RequestError: if the block document was not found for any reason

        Returns:
            A block document or None.
        """
        try:
            response = await self.request(
                "GET",
                f"/block_types/slug/{block_type_slug}/block_documents/name/{name}",
                params=dict(include_secrets=include_secrets),
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
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
    ) -> "BlockSchema | None":
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
        from prefect_cloud.schemas.objects import BlockSchema

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
    ) -> list["DeploymentResponse"]:
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
        from prefect_cloud.schemas.responses import DeploymentResponse

        body: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "sort": None,
        }

        response = await self.request("POST", "/deployments/filter", json=body)
        return validate_list(DeploymentResponse, response.json())

    async def create_deployment_schedule(
        self,
        deployment_id: "UUID",
        schedule: CronSchedule,
        active: bool,
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

    async def ensure_managed_work_pool(
        self, name: str = settings.default_managed_work_pool_name
    ) -> str:
        work_pools = await self.read_managed_work_pools()

        if work_pools:
            return work_pools[0].name

        template = await self.get_default_base_job_template_for_managed_work_pool()
        if template is None:
            raise ValueError("No default base job template found for managed work pool")

        work_pool = await self.create_work_pool_managed_by_name(
            name=name,
            template=template,
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
            await self.update_block_document_value(
                block_document_id=existing_block.id,
                value=credentials,
            )

        except ObjectNotFound:
            secret_block_type = await self.read_block_type_by_slug("secret")
            secret_block_schema = (
                await self.get_most_recent_block_schema_for_block_type(
                    block_type_id=secret_block_type.id
                )
            )
            if secret_block_schema is None:
                raise ValueError("No secret block schema found")

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

    async def pause_deployment(self, deployment_id: UUID):
        deployment = await self.read_deployment(deployment_id)

        for schedule in deployment.schedules:
            await self.update_deployment_schedule_active(
                deployment.id, schedule.id, active=False
            )

    async def resume_deployment(self, deployment_id: UUID):
        deployment = await self.read_deployment(deployment_id)

        for schedule in deployment.schedules:
            await self.update_deployment_schedule_active(
                deployment.id, schedule.id, active=True
            )

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
