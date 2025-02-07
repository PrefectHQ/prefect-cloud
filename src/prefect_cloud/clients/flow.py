from __future__ import annotations
from typing import TYPE_CHECKING, Any, Optional, List
from uuid import UUID
from datetime import datetime, timezone
from pydantic import BaseModel, Field
from pydantic_extra_types.pendulum_dt import DateTime
from httpx import HTTPStatusError, RequestError
from prefect_cloud.clients.base import BaseAsyncClient
from prefect_cloud.utilities.exception import ObjectNotFound
from prefect_cloud.utilities.generics import validate_list
from prefect_cloud.schemas.objects import FlowRun
from prefect_cloud.schemas.sorting import FlowRunSort

if TYPE_CHECKING:
    from prefect_cloud.schemas.objects import Flow


class FlowRunFilterExpectedStartTime(BaseModel):
    """Filter by `FlowRun.expected_start_time`."""

    before_: Optional[DateTime] = Field(
        default=None,
        description="Only include flow runs scheduled to start at or before this time",
    )
    after_: Optional[DateTime] = Field(
        default=None,
        description="Only include flow runs scheduled to start at or after this time",
    )


class FlowRunFilterDeploymentId(BaseModel):
    """Filter by `FlowRun.deployment_id`."""

    any_: Optional[List[UUID]] = Field(
        default=None, description="A list of flow run deployment ids to include"
    )


class FlowAsyncClient(BaseAsyncClient):
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

    async def read_flow(self, flow_id: "UUID") -> "Flow":
        """
        Query the Prefect API for a flow by id.

        Args:
            flow_id: the flow ID of interest

        Returns:
            a [Flow model][prefect.client.schemas.objects.Flow] representation of the flow
        """
        response = await self.request("GET", "/flows/{id}", path_params={"id": flow_id})
        from prefect_cloud.schemas.objects import Flow

        return Flow.model_validate(response.json())

    async def delete_flow(self, flow_id: "UUID") -> None:
        """
        Delete a flow by UUID.

        Args:
            flow_id: ID of the flow to be deleted
        Raises:
            prefect.exceptions.ObjectNotFound: If request returns 404
            httpx.RequestError: If requests fail
        """
        try:
            await self.request("DELETE", "/flows/{id}", path_params={"id": flow_id})
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def read_flow_by_name(
        self,
        flow_name: str,
    ) -> "Flow":
        """
        Query the Prefect API for a flow by name.

        Args:
            flow_name: the name of a flow

        Returns:
            a fully hydrated Flow model
        """
        response = await self.request(
            "GET", "/flows/name/{name}", path_params={"name": flow_name}
        )
        from prefect_cloud.schemas.objects import Flow

        return Flow.model_validate(response.json())

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

    async def read_next_scheduled_flow_runs_by_deployment_ids(
        self,
        deployment_ids: list[UUID],
        *,
        sort: "FlowRunSort | None" = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> "list[FlowRun]":
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

        expected_start_time = FlowRunFilterExpectedStartTime(
            after_=datetime.now(timezone.utc)
        )
        body: dict[str, Any] = {
            "deployment_id": FlowRunFilterDeploymentId(
                any_=deployment_ids
            ).model_dump_json(),
            "state": {"any_": ["SCHEDULED"]},
            "expected_start_time": expected_start_time.model_dump_json(),
            "sort": sort,
            "limit": limit,
            "offset": offset,
        }

        response = await self.request("POST", "/flow_runs/filter", json=body)

        from prefect_cloud.schemas.objects import FlowRun

        return validate_list(FlowRun, response.json())
