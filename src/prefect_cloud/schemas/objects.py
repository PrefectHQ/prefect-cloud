from __future__ import annotations


from typing import Annotated, Any, Optional, Literal
from uuid import UUID, uuid4
from pydantic import (
    Field,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    BaseModel,
)
from typing_extensions import TypeVar
from prefect_cloud.utilities.generics import handle_secret_render

from prefect_cloud.utilities.validators import (
    validate_block_document_name,
    validate_default_queue_id_not_none,
    default_timezone,
    validate_cron_string,
)

from prefect_cloud.types import (
    Name,
)
from prefect_cloud.utilities.generics import AutoEnum


DEFAULT_BLOCK_SCHEMA_VERSION: Literal["non-versioned"] = "non-versioned"
R = TypeVar("R", default=Any)


class WorkPoolStatus(AutoEnum):
    """Enumeration of work pool statuses."""

    READY = AutoEnum.auto()
    NOT_READY = AutoEnum.auto()
    PAUSED = AutoEnum.auto()

    @property
    def display_name(self) -> str:
        return self.name.replace("_", " ").capitalize()


class DeploymentFlowRun(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    deployment_id: Optional[UUID] = Field(
        default=None,
        description=(
            "The id of the deployment associated with this flow run, if available."
        ),
    )


class BlockType(BaseModel):
    """An ORM representation of a block type"""

    id: UUID = Field(default=..., description="The ID of the block type.")


class BlockSchema(BaseModel):
    """A representation of a block schema."""

    id: UUID = Field(default=..., description="The ID of the block schema.")


class BlockDocument(BaseModel):
    """An ORM representation of a block document."""

    id: UUID = Field(default=..., description="The ID of the block document.")
    name: Optional[Name] = Field(
        default=None,
        description=(
            "The block document's name. Not required for anonymous block documents."
        ),
    )
    data: dict[str, Any] = Field(
        default_factory=dict, description="The block document's data"
    )
    block_schema_id: UUID = Field(default=..., description="A block schema ID")
    block_schema: Optional[BlockSchema] = Field(
        default=None, description="The associated block schema"
    )
    block_type_id: UUID = Field(default=..., description="A block type ID")
    block_type_name: Optional[str] = Field(None, description="A block type name")
    block_type: Optional[BlockType] = Field(
        default=None, description="The associated block type"
    )

    _validate_name_format = field_validator("name")(validate_block_document_name)

    @model_serializer(mode="wrap")
    def serialize_data(
        self, handler: SerializerFunctionWrapHandler, info: SerializationInfo
    ) -> Any:
        self.data = handle_secret_render(self.data, info.context or {})
        return handler(self)


class Flow(BaseModel):
    """An ORM representation of flow data."""

    id: UUID = Field(default=..., description="The ID of the flow.")
    name: Name = Field(
        default=..., description="The name of the flow", examples=["my-flow"]
    )


class DeploymentSchedule(BaseModel):
    id: UUID = Field(
        default=...,
        description="The ID of the deployment schedule.",
    )
    deployment_id: Optional[UUID] = Field(
        default=None,
        description="The deployment id associated with this schedule.",
    )
    schedule: CronSchedule = Field(
        default=..., description="The schedule for the deployment."
    )
    active: bool = Field(
        default=True, description="Whether or not the schedule is active."
    )


class Deployment(BaseModel):
    """An ORM representation of deployment data."""

    name: Name = Field(default=..., description="The name of the deployment.")
    description: Optional[str] = Field(
        default=None, description="A description for the deployment."
    )
    flow_id: UUID = Field(
        default=..., description="The flow id associated with the deployment."
    )
    paused: bool = Field(
        default=False, description="Whether or not the deployment is paused."
    )
    schedules: list[DeploymentSchedule] = Field(
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
    work_queue_name: Optional[str] = Field(
        default=None,
        description=(
            "The work queue for the deployment. If no work queue is set, work will not"
            " be scheduled."
        ),
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


class WorkPool(BaseModel):
    """An ORM representation of a work pool"""

    name: Name = Field(
        description="The name of the work pool.",
    )
    type: str = Field(description="The work pool type.")
    base_job_template: dict[str, Any] = Field(
        default_factory=dict, description="The work pool's base job template."
    )
    is_paused: bool = Field(
        default=False,
        description="Pausing the work pool stops the delivery of all work.",
    )

    # this required field has a default of None so that the custom validator
    # below will be called and produce a more helpful error message. Because
    # the field metadata is attached via an annotation, the default is hidden
    # from type checkers.
    default_queue_id: Annotated[
        UUID, Field(default=None, description="The id of the pool's default queue.")
    ]

    @property
    def is_push_pool(self) -> bool:
        return self.type.endswith(":push")

    @property
    def is_managed_pool(self) -> bool:
        return self.type.endswith(":managed")

    @field_validator("default_queue_id")
    @classmethod
    def helpful_error_for_missing_default_queue_id(cls, v: Optional[UUID]) -> UUID:
        return validate_default_queue_id_not_none(v)


class CronSchedule(BaseModel):
    """
    Cron schedule

    NOTE: If the timezone is a DST-observing one, then the schedule will adjust
    itself appropriately. Cron's rules for DST are based on schedule times, not
    intervals. This means that an hourly cron schedule will fire on every new
    schedule hour, not every elapsed hour; for example, when clocks are set back
    this will result in a two-hour pause as the schedule will fire *the first
    time* 1am is reached and *the first time* 2am is reached, 120 minutes later.
    Longer schedules, such as one that fires at 9am every morning, will
    automatically adjust for DST.

    Args:
        cron (str): a valid cron string
        timezone (str): a valid timezone string in IANA tzdata format (for example,
            America/New_York).
        day_or (bool, optional): Control how croniter handles `day` and `day_of_week`
            entries. Defaults to True, matching cron which connects those values using
            OR. If the switch is set to False, the values are connected using AND. This
            behaves like fcron and enables you to e.g. define a job that executes each
            2nd friday of a month by setting the days of month and the weekday.

    """

    cron: str = Field(default=..., examples=["0 0 * * *"])
    timezone: Optional[str] = Field(default=None, examples=["America/New_York"])
    day_or: bool = Field(
        default=True,
        description=(
            "Control croniter behavior for handling day and day_of_week entries."
        ),
    )

    @field_validator("timezone")
    @classmethod
    def valid_timezone(cls, v: Optional[str]) -> Optional[str]:
        return default_timezone(v)

    @field_validator("cron")
    @classmethod
    def valid_cron_string(cls, v: str) -> str:
        return validate_cron_string(v)
