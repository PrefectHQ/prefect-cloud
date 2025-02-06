from typing import Any, Optional, Union
from copy import deepcopy

from uuid import UUID

from typing import Callable
from uuid import uuid4

import jsonschema
from pydantic import Field, field_validator, model_validator, HttpUrl, BaseModel

from prefect_cloud.schemas.objects import (
    ConcurrencyOptions,
    DEFAULT_BLOCK_SCHEMA_VERSION,
)


from prefect_cloud.utilities.validators import (
    convert_to_strings,
    remove_old_deployment_fields,
    validate_block_document_name,
    validate_block_type_slug,
    validate_name_present_on_nonanonymous_blocks,
    validate_schedule_max_scheduled_runs,
)

from prefect_cloud.schemas.schedules import SCHEDULE_TYPES
from prefect_cloud.types import (
    DateTime,
    KeyValueLabelsField,
    Name,
    NonEmptyishName,
    NonNegativeFloat,
    NonNegativeInteger,
    PositiveInteger,
)

from prefect_cloud.utilities.pydantic import get_class_fields_only

PREFECT_DEPLOYMENT_SCHEDULE_MAX_SCHEDULED_RUNS = 50


class FlowCreate(BaseModel):
    """Data used by the Prefect REST API to create a flow."""

    name: str = Field(
        default=..., description="The name of the flow", examples=["my-flow"]
    )
    tags: list[str] = Field(
        default_factory=list,
        description="A list of flow tags",
        examples=[["tag-1", "tag-2"]],
    )

    labels: KeyValueLabelsField = Field(default_factory=dict)


class DeploymentScheduleCreate(BaseModel):
    schedule: SCHEDULE_TYPES = Field(
        default=..., description="The schedule for the deployment."
    )
    active: bool = Field(
        default=True, description="Whether or not the schedule is active."
    )
    max_scheduled_runs: Optional[PositiveInteger] = Field(
        default=None,
        description="The maximum number of scheduled runs for the schedule.",
    )

    @field_validator("active", mode="wrap")
    @classmethod
    def validate_active(cls, v: Any, handler: Callable[[Any], Any]) -> bool:
        try:
            return handler(v)
        except Exception:
            raise ValueError(
                f"active must be able to be parsed as a boolean, got {v!r} of type {type(v)}"
            )

    @field_validator("max_scheduled_runs")
    @classmethod
    def validate_max_scheduled_runs(cls, v: Optional[int]) -> Optional[int]:
        return validate_schedule_max_scheduled_runs(
            v, PREFECT_DEPLOYMENT_SCHEDULE_MAX_SCHEDULED_RUNS
        )


class DeploymentScheduleUpdate(BaseModel):
    schedule: Optional[SCHEDULE_TYPES] = Field(
        default=None, description="The schedule for the deployment."
    )
    active: bool = Field(
        default=True, description="Whether or not the schedule is active."
    )

    max_scheduled_runs: Optional[PositiveInteger] = Field(
        default=None,
        description="The maximum number of scheduled runs for the schedule.",
    )

    @field_validator("max_scheduled_runs")
    @classmethod
    def validate_max_scheduled_runs(cls, v: Optional[int]) -> Optional[int]:
        return validate_schedule_max_scheduled_runs(
            v, PREFECT_DEPLOYMENT_SCHEDULE_MAX_SCHEDULED_RUNS
        )


class DeploymentCreate(BaseModel):
    """Data used by the Prefect REST API to create a deployment."""

    @model_validator(mode="before")
    @classmethod
    def remove_old_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        return remove_old_deployment_fields(values)

    @field_validator("description", "tags", mode="before")
    @classmethod
    def convert_to_strings(
        cls, values: Optional[Union[str, list[str]]]
    ) -> Union[str, list[str]]:
        return convert_to_strings(values)

    name: str = Field(..., description="The name of the deployment.")
    flow_id: UUID = Field(..., description="The ID of the flow to deploy.")
    paused: Optional[bool] = Field(default=None)
    schedules: list[DeploymentScheduleCreate] = Field(
        default_factory=list,
        description="A list of schedules for the deployment.",
    )
    concurrency_limit: Optional[int] = Field(
        default=None,
        description="The concurrency limit for the deployment.",
    )
    concurrency_options: Optional[ConcurrencyOptions] = Field(
        default=None,
        description="The concurrency options for the deployment.",
    )
    enforce_parameter_schema: Optional[bool] = Field(
        default=None,
        description=(
            "Whether or not the deployment should enforce the parameter schema."
        ),
    )
    parameter_openapi_schema: Optional[dict[str, Any]] = Field(default_factory=dict)
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters for flow runs scheduled by the deployment.",
    )
    tags: list[str] = Field(default_factory=list)
    labels: KeyValueLabelsField = Field(default_factory=dict)
    pull_steps: Optional[list[dict[str, Any]]] = Field(default=None)

    work_queue_name: Optional[str] = Field(default=None)
    work_pool_name: Optional[str] = Field(
        default=None,
        description="The name of the deployment's work pool.",
        examples=["my-work-pool"],
    )
    storage_document_id: Optional[UUID] = Field(default=None)
    infrastructure_document_id: Optional[UUID] = Field(default=None)
    description: Optional[str] = Field(default=None)
    path: Optional[str] = Field(default=None)
    version: Optional[str] = Field(default=None)
    entrypoint: Optional[str] = Field(default=None)
    job_variables: dict[str, Any] = Field(
        default_factory=dict,
        description="Overrides to apply to flow run infrastructure at runtime.",
    )

    def check_valid_configuration(self, base_job_template: dict[str, Any]) -> None:
        """Check that the combination of base_job_template defaults
        and job_variables conforms to the specified schema.
        """
        variables_schema = deepcopy(base_job_template.get("variables"))

        if variables_schema is not None:
            # jsonschema considers required fields, even if that field has a default,
            # to still be required. To get around this we remove the fields from
            # required if there is a default present.
            required = variables_schema.get("required")
            properties = variables_schema.get("properties")
            if required is not None and properties is not None:
                for k, v in properties.items():
                    if "default" in v and k in required:
                        required.remove(k)

            jsonschema.validate(self.job_variables, variables_schema)


class DeploymentUpdate(BaseModel):
    """Data used by the Prefect REST API to update a deployment."""

    @model_validator(mode="before")
    @classmethod
    def remove_old_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        return remove_old_deployment_fields(values)

    version: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    parameters: Optional[dict[str, Any]] = Field(
        default=None,
        description="Parameters for flow runs scheduled by the deployment.",
    )
    paused: Optional[bool] = Field(
        default=None, description="Whether or not the deployment is paused."
    )
    schedules: Optional[list[DeploymentScheduleCreate]] = Field(
        default=None,
        description="A list of schedules for the deployment.",
    )
    concurrency_limit: Optional[int] = Field(
        default=None,
        description="The concurrency limit for the deployment.",
    )
    concurrency_options: Optional[ConcurrencyOptions] = Field(
        default=None,
        description="The concurrency options for the deployment.",
    )
    tags: list[str] = Field(default_factory=list)
    work_queue_name: Optional[str] = Field(default=None)
    work_pool_name: Optional[str] = Field(
        default=None,
        description="The name of the deployment's work pool.",
        examples=["my-work-pool"],
    )
    path: Optional[str] = Field(default=None)
    job_variables: Optional[dict[str, Any]] = Field(
        default_factory=dict,
        description="Overrides to apply to flow run infrastructure at runtime.",
    )
    entrypoint: Optional[str] = Field(default=None)
    storage_document_id: Optional[UUID] = Field(default=None)
    infrastructure_document_id: Optional[UUID] = Field(default=None)
    enforce_parameter_schema: Optional[bool] = Field(
        default=None,
        description=(
            "Whether or not the deployment should enforce the parameter schema."
        ),
    )

    def check_valid_configuration(self, base_job_template: dict[str, Any]) -> None:
        """Check that the combination of base_job_template defaults
        and job_variables conforms to the specified schema.
        """
        variables_schema = deepcopy(base_job_template.get("variables"))

        if variables_schema is not None:
            # jsonschema considers required fields, even if that field has a default,
            # to still be required. To get around this we remove the fields from
            # required if there is a default present.
            required = variables_schema.get("required")
            properties = variables_schema.get("properties")
            if required is not None and properties is not None:
                for k, v in properties.items():
                    if "default" in v and k in required:
                        required.remove(k)

        if variables_schema is not None:
            jsonschema.validate(self.job_variables, variables_schema)


class BlockTypeCreate(BaseModel):
    """Data used by the Prefect REST API to create a block type."""

    name: str = Field(default=..., description="A block type's name")
    slug: str = Field(default=..., description="A block type's slug")
    logo_url: Optional[HttpUrl] = Field(
        default=None, description="Web URL for the block type's logo"
    )
    documentation_url: Optional[HttpUrl] = Field(
        default=None, description="Web URL for the block type's documentation"
    )
    description: Optional[str] = Field(
        default=None,
        description="A short blurb about the corresponding block's intended use",
    )
    code_example: Optional[str] = Field(
        default=None,
        description="A code snippet demonstrating use of the corresponding block",
    )

    # validators
    _validate_slug_format = field_validator("slug")(validate_block_type_slug)


class BlockTypeUpdate(BaseModel):
    """Data used by the Prefect REST API to update a block type."""

    logo_url: Optional[HttpUrl] = Field(default=None)
    documentation_url: Optional[HttpUrl] = Field(default=None)
    description: Optional[str] = Field(default=None)
    code_example: Optional[str] = Field(default=None)

    @classmethod
    def updatable_fields(cls) -> set[str]:
        return get_class_fields_only(cls)


class BlockSchemaCreate(BaseModel):
    """Data used by the Prefect REST API to create a block schema."""

    fields: dict[str, Any] = Field(
        default_factory=dict, description="The block schema's field schema"
    )
    block_type_id: Optional[UUID] = Field(default=None)
    capabilities: list[str] = Field(
        default_factory=list,
        description="A list of Block capabilities",
    )
    version: str = Field(
        default=DEFAULT_BLOCK_SCHEMA_VERSION,
        description="Human readable identifier for the block schema",
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
    is_anonymous: bool = Field(
        default=False,
        description=(
            "Whether the block is anonymous (anonymous blocks are usually created by"
            " Prefect automatically)"
        ),
    )

    _validate_name_format = field_validator("name")(validate_block_document_name)

    @model_validator(mode="before")
    def validate_name_is_present_if_not_anonymous(
        cls, values: dict[str, Any]
    ) -> dict[str, Any]:
        return validate_name_present_on_nonanonymous_blocks(values)


class BlockDocumentUpdate(BaseModel):
    """Data used by the Prefect REST API to update a block document."""

    block_schema_id: Optional[UUID] = Field(
        default=None, description="A block schema ID"
    )
    data: dict[str, Any] = Field(
        default_factory=dict, description="The block document's data"
    )
    merge_existing_data: bool = Field(
        default=True,
        description="Whether to merge the existing data with the new data or replace it",
    )


class BlockDocumentReferenceCreate(BaseModel):
    """Data used to create block document reference."""

    id: UUID = Field(default_factory=uuid4)
    parent_block_document_id: UUID = Field(
        default=..., description="ID of block document the reference is nested within"
    )
    reference_block_document_id: UUID = Field(
        default=..., description="ID of the nested block document"
    )
    name: str = Field(
        default=..., description="The name that the reference is nested under"
    )


class WorkPoolCreate(BaseModel):
    """Data used by the Prefect REST API to create a work pool."""

    name: NonEmptyishName = Field(
        description="The name of the work pool.",
    )
    description: Optional[str] = Field(default=None)
    type: str = Field(
        description="The work pool type.", default="prefect-agent"
    )  # TODO: change default
    base_job_template: dict[str, Any] = Field(
        default_factory=dict,
        description="The base job template for the work pool.",
    )
    is_paused: bool = Field(
        default=False,
        description="Whether the work pool is paused.",
    )
    concurrency_limit: Optional[NonNegativeInteger] = Field(
        default=None, description="A concurrency limit for the work pool."
    )


class WorkPoolUpdate(BaseModel):
    """Data used by the Prefect REST API to update a work pool."""

    description: Optional[str] = Field(default=None)
    is_paused: Optional[bool] = Field(default=None)
    base_job_template: Optional[dict[str, Any]] = Field(default=None)
    concurrency_limit: Optional[int] = Field(default=None)


class WorkQueueCreate(BaseModel):
    """Data used by the Prefect REST API to create a work queue."""

    name: str = Field(default=..., description="The name of the work queue.")
    description: Optional[str] = Field(default=None)
    is_paused: bool = Field(
        default=False,
        description="Whether the work queue is paused.",
    )
    concurrency_limit: Optional[NonNegativeInteger] = Field(
        default=None,
        description="A concurrency limit for the work queue.",
    )
    priority: Optional[PositiveInteger] = Field(
        default=None,
        description=(
            "The queue's priority. Lower values are higher priority (1 is the highest)."
        ),
    )


class WorkQueueUpdate(BaseModel):
    """Data used by the Prefect REST API to update a work queue."""

    name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    is_paused: bool = Field(
        default=False, description="Whether or not the work queue is paused."
    )
    concurrency_limit: Optional[NonNegativeInteger] = Field(default=None)
    priority: Optional[PositiveInteger] = Field(
        None, description="The queue's priority."
    )
    last_polled: Optional[DateTime] = Field(default=None)


class GlobalConcurrencyLimitCreate(BaseModel):
    """Data used by the Prefect REST API to create a global concurrency limit."""

    name: Name = Field(description="The name of the global concurrency limit.")
    limit: NonNegativeInteger = Field(
        description=(
            "The maximum number of slots that can be occupied on this concurrency"
            " limit."
        )
    )
    active: Optional[bool] = Field(
        default=True,
        description="Whether or not the concurrency limit is in an active state.",
    )
    active_slots: Optional[NonNegativeInteger] = Field(
        default=0,
        description="Number of tasks currently using a concurrency slot.",
    )
    slot_decay_per_second: Optional[NonNegativeFloat] = Field(
        default=0.0,
        description=(
            "Controls the rate at which slots are released when the concurrency limit"
            " is used as a rate limit."
        ),
    )


class GlobalConcurrencyLimitUpdate(BaseModel):
    """Data used by the Prefect REST API to update a global concurrency limit."""

    name: Optional[Name] = Field(default=None)
    limit: Optional[NonNegativeInteger] = Field(default=None)
    active: Optional[bool] = Field(default=None)
    active_slots: Optional[NonNegativeInteger] = Field(default=None)
    slot_decay_per_second: Optional[NonNegativeFloat] = Field(default=None)
