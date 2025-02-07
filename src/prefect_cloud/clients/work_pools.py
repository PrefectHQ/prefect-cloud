from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from httpx import HTTPStatusError

from prefect_cloud.clients.base import BaseAsyncClient
from prefect_cloud.utilities.generics import validate_list

if TYPE_CHECKING:
    from prefect_cloud.schemas.objects import WorkPool


from prefect_cloud.utilities.exception import ObjectAlreadyExists, ObjectNotFound


PREFECT_MANAGED = "prefect:managed"


class WorkPoolAsyncClient(BaseAsyncClient):
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
            "work_pools": {"any_": [PREFECT_MANAGED]},
        }
        response = await self.request("POST", "/work_pools/filter", json=body)
        return validate_list(WorkPool, response.json())

    async def read_work_pool_by_name(self, name: str) -> "WorkPool":
        response = await self.request(
            "GET", "/work_pools/{name}", path_params={"name": name}
        )
        return WorkPool.model_validate(response.json())

    async def create_work_pool_managed_by_name(
        self,
        name: str,
        template: dict[str, Any],
        overwrite: bool = False,
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
        except HTTPStatusError as e:
            if e.response.status_code == 409:
                if overwrite:
                    existing_work_pool = await self.read_work_pool_by_name(name=name)
                    if existing_work_pool.type != PREFECT_MANAGED:
                        warnings.warn(
                            "Overwriting work pool type is not supported. Ignoring provided type.",
                            category=UserWarning,
                        )
                    await self.update_work_pool_type_and_template(
                        name=existing_work_pool.name,
                        type=PREFECT_MANAGED,
                        template=template,
                    )
                    response = await self.request(
                        "GET",
                        "/work_pools/{name}",
                        path_params={"name": name},
                    )
                else:
                    raise ObjectAlreadyExists(http_exc=e) from e
            else:
                raise

        return WorkPool.model_validate(response.json())

    async def update_work_pool_type_and_template(
        self,
        name: str,
        type: str,
        template: dict[str, Any],
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
                path_params={"name": name},
                json={"type": type, "base_job_template": template},
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise
