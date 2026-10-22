from pydantic import BaseModel, Field

import prefect_cloud.schemas.objects as objects


class DeploymentResponse(objects.Deployment):
    work_pool_name: str = Field(default=..., description="The name of the work pool.")


class GitHubStateTokenResponse(BaseModel):
    state_token: str
    authorize_url: str | None = Field(
        default=None,
        description="GitHub's OAuth authorize page for the Prefect Cloud app. "
        "Servers that predate it omit this field.",
    )
