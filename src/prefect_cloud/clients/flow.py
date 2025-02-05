from __future__ import annotations

from typing import TYPE_CHECKING

from httpx import HTTPStatusError, RequestError

from prefect_cloud.clients.base import BaseAsyncClient
from prefect_cloud.utilities.exception import ObjectNotFound

if TYPE_CHECKING:
    from uuid import UUID

    from prefect_cloud.schemas.objects import (
        Flow,
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
        from prefect.client.schemas.actions import FlowCreate

        flow_data = FlowCreate(name=flow_name)
        response = await self.request(
            "POST", "/flows/", json=flow_data.model_dump(mode="json")
        )

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
