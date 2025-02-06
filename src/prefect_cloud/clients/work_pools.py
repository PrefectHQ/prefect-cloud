from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from httpx import HTTPStatusError

from prefect_cloud.clients.base import BaseAsyncClient
from prefect_cloud.utilities.generics import validate_list

if TYPE_CHECKING:
    from prefect_cloud.schemas.actions import (
        WorkPoolCreate,
        WorkPoolUpdate,
    )
    from prefect_cloud.schemas.filters import WorkPoolFilter
    from prefect_cloud.schemas.objects import WorkPool


from prefect_cloud.utilities.exception import ObjectAlreadyExists, ObjectNotFound


class WorkPoolAsyncClient(BaseAsyncClient):
    async def read_work_pool(self, work_pool_name: str) -> WorkPool:
        """
        Reads information for a given work pool
        Args:
            work_pool_name: The name of the work pool to for which to get
                information.
        Returns:
            Information about the requested work pool.
        """
        try:
            response = await self.request(
                "GET",
                "/work_pools/{name}",
                path_params={"work_pool_name": work_pool_name},
            )
            response.raise_for_status()
            return WorkPool.model_validate(response.json())
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def read_work_pools(
        self,
        limit: int | None = None,
        offset: int = 0,
        work_pool_filter: "WorkPoolFilter | None" = None,
    ) -> list["WorkPool"]:
        """
        Reads work pools.

        Args:
            limit: Limit for the work pool query.
            offset: Offset for the work pool query.
            work_pool_filter: Criteria by which to filter work pools.

        Returns:
            A list of work pools.
        """
        from prefect_cloud.schemas.objects import WorkPool

        body: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "work_pools": (
                work_pool_filter.model_dump(mode="json") if work_pool_filter else None
            ),
        }
        response = await self.request("POST", "/work_pools/filter", json=body)
        return validate_list(WorkPool, response.json())

    async def create_work_pool(
        self,
        work_pool: "WorkPoolCreate",
        overwrite: bool = False,
    ) -> "WorkPool":
        """
        Creates a work pool with the provided configuration.

        Args:
            work_pool: Desired configuration for the new work pool.

        Returns:
            Information about the newly created work pool.
        """
        from prefect_cloud.schemas.actions import WorkPoolUpdate
        from prefect_cloud.schemas.objects import WorkPool

        try:
            response = await self.request(
                "POST",
                "/work_pools/",
                json=work_pool.model_dump(mode="json", exclude_unset=True),
            )
        except HTTPStatusError as e:
            if e.response.status_code == 409:
                if overwrite:
                    existing_work_pool = await self.read_work_pool(
                        work_pool_name=work_pool.name
                    )
                    if existing_work_pool.type != work_pool.type:
                        warnings.warn(
                            "Overwriting work pool type is not supported. Ignoring provided type.",
                            category=UserWarning,
                        )
                    await self.update_work_pool(
                        work_pool_name=work_pool.name,
                        work_pool=WorkPoolUpdate.model_validate(
                            work_pool.model_dump(exclude={"name", "type"})
                        ),
                    )
                    response = await self.request(
                        "GET",
                        "/work_pools/{name}",
                        path_params={"name": work_pool.name},
                    )
                else:
                    raise ObjectAlreadyExists(http_exc=e) from e
            else:
                raise

        return WorkPool.model_validate(response.json())

    async def update_work_pool(
        self,
        work_pool_name: str,
        work_pool: "WorkPoolUpdate",
    ) -> None:
        """
        Updates a work pool.

        Args:
            work_pool_name: Name of the work pool to update.
            work_pool: Fields to update in the work pool.
        """
        try:
            await self.request(
                "PATCH",
                "/work_pools/{name}",
                path_params={"name": work_pool_name},
                json=work_pool.model_dump(mode="json", exclude_unset=True),
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def delete_work_pool(
        self,
        work_pool_name: str,
    ) -> None:
        """
        Deletes a work pool.

        Args:
            work_pool_name: Name of the work pool to delete.
        """
        try:
            await self.request(
                "DELETE",
                "/work_pools/{name}",
                path_params={"name": work_pool_name},
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise
