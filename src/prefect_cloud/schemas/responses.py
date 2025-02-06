from typing import Any, Optional
from uuid import UUID

from pydantic import Field, BaseModel

import prefect_cloud.schemas.objects as objects

from prefect_cloud.schemas.fields import CreatedBy, UpdatedBy
from prefect_cloud.types import DateTime, KeyValueLabelsField


class GlobalConcurrencyLimitResponse(BaseModel):
    """
    A response object for global concurrency limits.
    """

    active: bool = Field(
        default=True, description="Whether the global concurrency limit is active."
    )
    name: str = Field(
        default=..., description="The name of the global concurrency limit."
    )
    limit: int = Field(default=..., description="The concurrency limit.")
    active_slots: int = Field(default=..., description="The number of active slots.")
    slot_decay_per_second: float = Field(
        default=2.0,
        description="The decay rate for active slots when used as a rate limit.",
    )


class DeploymentResponse(BaseModel):
    name: str = Field(default=..., description="The name of the deployment.")
    version: Optional[str] = Field(
        default=None, description="An optional version for the deployment."
    )
    description: Optional[str] = Field(
        default=None, description="A description for the deployment."
    )
    flow_id: UUID = Field(
        default=..., description="The flow id associated with the deployment."
    )
    concurrency_limit: Optional[int] = Field(
        default=None,
        description="DEPRECATED: Prefer `global_concurrency_limit`. Will always be None for backwards compatibility. Will be removed after December 2024.",
        deprecated=True,
    )
    global_concurrency_limit: Optional["GlobalConcurrencyLimitResponse"] = Field(
        default=None,
        description="The global concurrency limit object for enforcing the maximum number of flow runs that can be active at once.",
    )
    concurrency_options: Optional[objects.ConcurrencyOptions] = Field(
        default=None,
        description="The concurrency options for the deployment.",
    )
    paused: bool = Field(
        default=False, description="Whether or not the deployment is paused."
    )
    concurrency_options: Optional[objects.ConcurrencyOptions] = Field(
        default=None,
        description="The concurrency options for the deployment.",
    )
    schedules: list[objects.DeploymentSchedule] = Field(
        default_factory=list, description="A list of schedules for the deployment."
    )
    job_variables: dict[str, Any] = Field(
        default_factory=dict,
        description="Overrides to apply to flow run infrastructure at runtime.",
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters for flow runs scheduled by the deployment.",
    )
    pull_steps: Optional[list[dict[str, Any]]] = Field(
        default=None,
        description="Pull steps for cloning and running this deployment.",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="A list of tags for the deployment",
        examples=[["tag-1", "tag-2"]],
    )
    labels: KeyValueLabelsField
    work_queue_name: Optional[str] = Field(
        default=None,
        description=(
            "The work queue for the deployment. If no work queue is set, work will not"
            " be scheduled."
        ),
    )
    last_polled: Optional[DateTime] = Field(
        default=None,
        description="The last time the deployment was polled for status updates.",
    )
    parameter_openapi_schema: Optional[dict[str, Any]] = Field(
        default=None,
        description="The parameter schema of the flow, including defaults.",
    )
    path: Optional[str] = Field(
        default=None,
        description=(
            "The path to the working directory for the workflow, relative to remote"
            " storage or an absolute path."
        ),
    )
    entrypoint: Optional[str] = Field(
        default=None,
        description=(
            "The path to the entrypoint for the workflow, relative to the `path`."
        ),
    )
    storage_document_id: Optional[UUID] = Field(
        default=None,
        description="The block document defining storage used for this flow.",
    )
    infrastructure_document_id: Optional[UUID] = Field(
        default=None,
        description="The block document defining infrastructure to use for flow runs.",
    )
    created_by: Optional[CreatedBy] = Field(
        default=None,
        description="Optional information about the creator of this deployment.",
    )
    updated_by: Optional[UpdatedBy] = Field(
        default=None,
        description="Optional information about the updater of this deployment.",
    )
    work_queue_id: Optional[UUID] = Field(
        default=None,
        description=(
            "The id of the work pool queue to which this deployment is assigned."
        ),
    )
    enforce_parameter_schema: bool = Field(
        default=True,
        description=(
            "Whether or not the deployment should enforce the parameter schema."
        ),
    )
    work_pool_name: Optional[str] = Field(
        default=None,
        description="The name of the deployment's work pool.",
    )
    status: Optional[objects.DeploymentStatus] = Field(
        default=None,
        description="Current status of the deployment.",
    )
