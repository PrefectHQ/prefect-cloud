from uuid import UUID

from pydantic import BaseModel, Field

import prefect_cloud.schemas.objects as objects


class DeploymentResponse(BaseModel):
    id: UUID = Field(default=..., description="The ID of the deployment.")
    name: str = Field(default=..., description="The name of the deployment.")
    flow_id: UUID = Field(default=..., description="The ID of the flow.")
    schedules: list[objects.DeploymentSchedule] = Field(
        default_factory=list, description="A list of schedules for the deployment."
    )
    work_pool_name: str = Field(default=..., description="The name of the work pool.")
