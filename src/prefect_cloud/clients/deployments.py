from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

from httpx import HTTPStatusError, RequestError
from prefect_cloud.utilities.exception import ObjectNotFound
from prefect_cloud.clients.base import BaseAsyncClient
from prefect_cloud.utilities.generics import validate_list

if TYPE_CHECKING:
    from uuid import UUID
    from prefect_cloud.schemas.actions import (
        DeploymentUpdate,
    )

    from prefect_cloud.schemas.objects import DeploymentSchedule, FlowRun
    from prefect_cloud.schemas.responses import DeploymentResponse
    from prefect_cloud.schemas.schedules import SCHEDULE_TYPES


class DeploymentAsyncClient(BaseAsyncClient):
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
        from uuid import UUID
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

        # Exclude newer fields that are not set to avoid compatibility issues
        exclude = {
            field
            for field in ["work_pool_name"]
            if field not in deployment_create.model_fields_set
        }

        if deployment_create.pull_steps is None:
            exclude.add("pull_steps")

        json = deployment_create.model_dump(mode="json", exclude=exclude)
        response = await self.request(
            "POST",
            "/deployments/",
            json=json,
        )
        deployment_id = response.json().get("id")
        if not deployment_id:
            raise RequestError(f"Malformed response: {response}")

        return UUID(deployment_id)

    async def set_deployment_paused_state(
        self, deployment_id: "UUID", paused: bool
    ) -> None:
        await self.request(
            "PATCH",
            "/deployments/{id}",
            path_params={"id": deployment_id},
            json={"paused": paused},
        )

    async def update_deployment(
        self,
        deployment_id: "UUID",
        deployment: "DeploymentUpdate",
    ) -> None:
        await self.request(
            "PATCH",
            "/deployments/{id}",
            path_params={"id": deployment_id},
            json=deployment.model_dump(mode="json", exclude_unset=True),
        )

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
                "/deployments/{id}",
                path_params={"id": deployment_id},
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
                "/deployments/name/{flow_name}/{deployment_name}",
                path_params={
                    "flow_name": flow_name,
                    "deployment_name": deployment_name,
                },
            )
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

    async def delete_deployment(
        self,
        deployment_id: "UUID",
    ) -> None:
        """
        Delete deployment by id.

        Args:
            deployment_id: The deployment id of interest.
        Raises:
            ObjectNotFound: If request returns 404
            RequestError: If requests fails
        """
        try:
            await self.request(
                "DELETE",
                "/deployments/{id}",
                path_params={"id": deployment_id},
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def create_deployment_schedules(
        self,
        deployment_id: "UUID",
        schedules: list[tuple["SCHEDULE_TYPES", bool]],
    ) -> list["DeploymentSchedule"]:
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

        deployment_schedule_create = [
            DeploymentScheduleCreate(schedule=schedule[0], active=schedule[1])
            for schedule in schedules
        ]

        json = [
            deployment_schedule_create.model_dump(mode="json")
            for deployment_schedule_create in deployment_schedule_create
        ]
        response = await self.request(
            "POST",
            "/deployments/{id}/schedules",
            path_params={"id": deployment_id},
            json=json,
        )
        return validate_list(DeploymentSchedule, response.json())

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
                "/deployments/{id}/schedules",
                path_params={"id": deployment_id},
            )
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
            await self.request(
                "PATCH",
                "/deployments/{id}/schedules/{schedule_id}",
                path_params={"id": deployment_id, "schedule_id": schedule_id},
                json={"active": active},
            )
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
            await self.request(
                "DELETE",
                "/deployments/{id}/schedules/{schedule_id}",
                path_params={"id": deployment_id, "schedule_id": schedule_id},
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def create_flow_run_from_deployment_id(
        self,
        deployment_id: "UUID",
    ) -> "FlowRun":
        """
        Create a flow run for a deployment.

        Args:
            deployment_id: The deployment ID to create the flow run from

        Raises:
            RequestError: if the Prefect API does not successfully create a run for any reason

        Returns:
            The flow run model
        """

        from prefect_cloud.schemas.objects import FlowRun
        from prefect_cloud.schemas.objects import StateType

        response = await self.request(
            "POST",
            "/deployments/{id}/create_flow_run",
            path_params={"id": deployment_id},
            json={"state": {"type": StateType.SCHEDULED}},
        )
        return FlowRun.model_validate(response.json())
