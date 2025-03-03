from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from prefect_cloud.schemas.objects import CronSchedule
from prefect_cloud.types import (
    Name,
    NonEmptyishName,
)


class DeploymentScheduleCreate(BaseModel):
    schedule: CronSchedule = Field(
        default=..., description="The schedule for the deployment."
    )
    active: bool = Field(
        default=True, description="Whether or not the schedule is active."
    )
    parameters: Optional[dict[str, Any]] = Field(
        default_factory=dict, description="The parameters for the schedule."
    )


class DeploymentCreate(BaseModel):
    """Data used by the Prefect REST API to create a deployment."""

    name: str = Field(..., description="The name of the deployment.")
    flow_id: UUID = Field(..., description="The ID of the flow to deploy.")
    entrypoint: Optional[str] = Field(default=None)
    work_pool_name: Optional[str] = Field(
        default=None,
        description="The name of the deployment's work pool.",
        examples=["my-work-pool"],
    )
    pull_steps: Optional[list[dict[str, Any]]] = Field(default=None)
    parameter_openapi_schema: dict[str, Any] = Field(default_factory=dict)
    job_variables: dict[str, Any] = Field(
        default_factory=dict,
        description="Overrides to apply to flow run infrastructure at runtime.",
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Default parameter values to pass to the flow at runtime.",
    )


class BlockDocumentCreate(BaseModel):
    """Data used by the Prefect REST API to create a block document."""

    name: Optional[Name] = Field(
        default=None, description="The name of the block document"
    )
    data: dict[str, Any] = Field(
        default_factory=dict, description="The block document's data"
    )
    block_schema_id: UUID = Field(
        default=..., description="The block schema ID for the block document"
    )
    block_type_id: UUID = Field(
        default=..., description="The block type ID for the block document"
    )


class WorkPoolCreate(BaseModel):
    """Data used by the Prefect REST API to create a work pool."""

    name: NonEmptyishName = Field(
        description="The name of the work pool.",
    )
    type: str = Field(
        description="The work pool type.", default="prefect-agent"
    )  # TODO: change default
    base_job_template: dict[str, Any] = Field(
        default_factory=dict,
        description="The base job template for the work pool.",
    )


class DeploymentFlowRunCreate(BaseModel):
    """Data used by the Prefect REST API to create a flow run from a deployment."""

    name: Optional[str] = Field(default=None, description="The name of the flow run.")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="The parameters for the flow run."
    )
    enforce_parameter_schema: Optional[bool] = Field(
        default=None,
        description="Whether or not to enforce the parameter schema on this run.",
    )
    work_queue_name: Optional[str] = Field(default=None)
    job_variables: Optional[dict[str, Any]] = Field(default=None)
