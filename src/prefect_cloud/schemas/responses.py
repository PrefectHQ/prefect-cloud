from pydantic import Field

import prefect_cloud.schemas.objects as objects


class DeploymentResponse(objects.Deployment):
    work_pool_name: str = Field(default=..., description="The name of the work pool.")
