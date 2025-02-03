from typing import Any
from uuid import UUID, uuid4

from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.client.schemas.filters import WorkPoolFilter, WorkPoolFilterType
from prefect.settings import (
    PREFECT_API_KEY,
    PREFECT_API_URL,
)
from prefect.utilities.callables import ParameterSchema
from prefect.workers.utilities import (
    get_default_base_job_template_for_infrastructure_type,
)
from prefect_cloud.settings import settings

PREFECT_MANAGED = "prefect:managed"

#TODO: temporary remove
def get_cloud_api_url():
    url = PREFECT_API_URL.value()
    if url.startswith("https://api.prefect.dev/api"):
        return "https://api.prefect.dev/api"
    elif url.startswith("https://api.stg.prefect.dev/api"):
        return "https://api.stg.prefect.dev/api"
    else:
        return "https://api.prefect.cloud/api"


class PrefectCloudClient(PrefectClient):
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



def get_prefect_cloud_client():
    return PrefectCloudClient(
        api=PREFECT_API_URL.value(),
        api_key=PREFECT_API_KEY.value(),
    )
