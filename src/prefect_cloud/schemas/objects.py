from __future__ import annotations


from functools import partial
from typing import Annotated, Any, Optional, Literal, Generic
import datetime
from uuid import UUID, uuid4
from pydantic import (
    Field,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
    BaseModel,
)
from typing_extensions import TypeVar
from prefect_cloud.utilities.pydantic import handle_secret_render

from prefect_cloud.utilities.validators import (
    validate_block_document_name,
    validate_default_queue_id_not_none,
    validate_name_present_on_nonanonymous_blocks,
    get_or_create_run_name,
)
from prefect_cloud.schemas.schedules import SCHEDULE_TYPES
from prefect_cloud.types import (
    KeyValueLabelsField,
    Name,
    NonNegativeInteger,
    PositiveInteger,
)
from prefect_cloud.types import DateTime
from prefect_cloud.utilities.collections import AutoEnum, visit_collection


from prefect_cloud.utilities.names import generate_slug

DEFAULT_BLOCK_SCHEMA_VERSION: Literal["non-versioned"] = "non-versioned"
R = TypeVar("R", default=Any)


class StateType(AutoEnum):
    """Enumeration of state types."""

    SCHEDULED = AutoEnum.auto()
    PENDING = AutoEnum.auto()
    RUNNING = AutoEnum.auto()
    COMPLETED = AutoEnum.auto()
    FAILED = AutoEnum.auto()
    CANCELLED = AutoEnum.auto()
    CRASHED = AutoEnum.auto()
    PAUSED = AutoEnum.auto()
    CANCELLING = AutoEnum.auto()


class WorkPoolStatus(AutoEnum):
    """Enumeration of work pool statuses."""

    READY = AutoEnum.auto()
    NOT_READY = AutoEnum.auto()
    PAUSED = AutoEnum.auto()

    @property
    def display_name(self) -> str:
        return self.name.replace("_", " ").capitalize()


class WorkerStatus(AutoEnum):
    """Enumeration of worker statuses."""

    ONLINE = AutoEnum.auto()
    OFFLINE = AutoEnum.auto()


class DeploymentStatus(AutoEnum):
    """Enumeration of deployment statuses."""

    READY = AutoEnum.auto()
    NOT_READY = AutoEnum.auto()


class WorkQueueStatus(AutoEnum):
    """Enumeration of work queue statuses."""

    READY = AutoEnum.auto()
    NOT_READY = AutoEnum.auto()
    PAUSED = AutoEnum.auto()


class ConcurrencyLimitStrategy(AutoEnum):
    """Enumeration of concurrency limit strategies."""

    ENQUEUE = AutoEnum.auto()
    CANCEL_NEW = AutoEnum.auto()


class ConcurrencyOptions(BaseModel):
    """
    Class for storing the concurrency config in database.
    """

    collision_strategy: ConcurrencyLimitStrategy


class ConcurrencyLimitConfig(BaseModel):
    """
    Class for storing the concurrency limit config in database.
    """

    limit: int
    collision_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.ENQUEUE


class StateDetails(BaseModel):
    flow_run_id: Optional[UUID] = None
    task_run_id: Optional[UUID] = None
    # for task runs that represent subflows, the subflow's run ID
    child_flow_run_id: Optional[UUID] = None
    scheduled_time: Optional[DateTime] = None
    cache_key: Optional[str] = None
    cache_expiration: Optional[DateTime] = None
    deferred: Optional[bool] = None
    untrackable_result: bool = False
    pause_timeout: Optional[DateTime] = None
    pause_reschedule: bool = False
    pause_key: Optional[str] = None
    run_input_keyset: Optional[dict[str, str]] = None
    refresh_cache: Optional[bool] = None
    retriable: Optional[bool] = None
    transition_id: Optional[UUID] = None
    task_parameters_id: Optional[UUID] = None
    # Captures the trace_id and span_id of the span where this state was created
    traceparent: Optional[str] = None


def data_discriminator(x: Any) -> str:
    if isinstance(x, dict) and "storage_key" in x:
        return "ResultRecordMetadata"
    return "Any"


class State(BaseModel, Generic[R]):
    """
    The state of a run.
    """

    type: StateType
    name: Optional[str] = Field(default=None)
    timestamp: DateTime = Field(default_factory=lambda: DateTime.now("UTC"))
    message: Optional[str] = Field(default=None, examples=["Run started"])
    state_details: StateDetails = Field(default_factory=StateDetails)


class FlowRun(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str = Field(
        default_factory=lambda: generate_slug(2),
        description=(
            "The name of the flow run. Defaults to a random slug if not specified."
        ),
        examples=["my-flow-run"],
    )
    flow_id: UUID = Field(default=..., description="The id of the flow being run.")
    state_id: Optional[UUID] = Field(
        default=None, description="The id of the flow run's current state."
    )
    deployment_id: Optional[UUID] = Field(
        default=None,
        description=(
            "The id of the deployment associated with this flow run, if available."
        ),
    )
    deployment_version: Optional[str] = Field(
        default=None,
        description="The version of the deployment associated with this flow run.",
        examples=["1.0"],
    )
    work_queue_name: Optional[str] = Field(
        default=None, description="The work queue that handled this flow run."
    )
    flow_version: Optional[str] = Field(
        default=None,
        description="The version of the flow executed in this flow run.",
        examples=["1.0"],
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Parameters for the flow run."
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        description=(
            "An optional idempotency key for the flow run. Used to ensure the same flow"
            " run is not created multiple times."
        ),
    )
    context: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional context for the flow run.",
        examples=[{"my_var": "my_val"}],
    )
    tags: list[str] = Field(
        default_factory=list,
        description="A list of tags on the flow run",
        examples=[["tag-1", "tag-2"]],
    )
    labels: KeyValueLabelsField = Field(default_factory=dict)
    parent_task_run_id: Optional[UUID] = Field(
        default=None,
        description=(
            "If the flow run is a subflow, the id of the 'dummy' task in the parent"
            " flow used to track subflow state."
        ),
    )
    run_count: int = Field(
        default=0, description="The number of times the flow run was executed."
    )
    expected_start_time: Optional[DateTime] = Field(
        default=None,
        description="The flow run's expected start time.",
    )
    next_scheduled_start_time: Optional[DateTime] = Field(
        default=None,
        description="The next time the flow run is scheduled to start.",
    )
    start_time: Optional[DateTime] = Field(
        default=None, description="The actual start time."
    )
    end_time: Optional[DateTime] = Field(
        default=None, description="The actual end time."
    )
    total_run_time: datetime.timedelta = Field(
        default=datetime.timedelta(0),
        description=(
            "Total run time. If the flow run was executed multiple times, the time of"
            " each run will be summed."
        ),
    )
    estimated_run_time: datetime.timedelta = Field(
        default=datetime.timedelta(0),
        description="A real-time estimate of the total run time.",
    )
    estimated_start_time_delta: datetime.timedelta = Field(
        default=datetime.timedelta(0),
        description="The difference between actual and expected start time.",
    )

    work_queue_id: Optional[UUID] = Field(
        default=None, description="The id of the run's work pool queue."
    )

    work_pool_id: Optional[UUID] = Field(
        default=None, description="The work pool with which the queue is associated."
    )
    work_pool_name: Optional[str] = Field(
        default=None,
        description="The name of the flow run's work pool.",
        examples=["my-work-pool"],
    )
    state: Optional[State] = Field(
        default=None,
        description="The state of the flow run.",
        examples=["State(type=StateType.COMPLETED)"],
    )
    job_variables: Optional[dict[str, Any]] = Field(
        default=None,
        description="Job variables for the flow run.",
    )

    def __eq__(self, other: Any) -> bool:
        """
        Check for "equality" to another flow run schema

        Estimates times are rolling and will always change with repeated queries for
        a flow run so we ignore them during equality checks.
        """
        if isinstance(other, FlowRun):
            exclude_fields = {"estimated_run_time", "estimated_start_time_delta"}
            return self.model_dump(exclude=exclude_fields) == other.model_dump(
                exclude=exclude_fields
            )
        return super().__eq__(other)

    @field_validator("name", mode="before")
    @classmethod
    def set_default_name(cls, name: Optional[str]) -> str:
        return get_or_create_run_name(name)


class BlockType(BaseModel):
    """An ORM representation of a block type"""

    id: UUID = Field(default=..., description="The ID of the block type.")
    name: Name = Field(default=..., description="A block type's name")
    slug: str = Field(default=..., description="A block type's slug")


class BlockSchema(BaseModel):
    """A representation of a block schema."""

    id: UUID = Field(default=..., description="The ID of the block schema.")
    checksum: str = Field(default=..., description="The block schema's unique checksum")
    fields: dict[str, Any] = Field(
        default_factory=dict, description="The block schema's field schema"
    )
    block_type_id: Optional[UUID] = Field(default=..., description="A block type ID")
    block_type: Optional[BlockType] = Field(
        default=None, description="The associated block type"
    )


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

    @model_validator(mode="before")
    @classmethod
    def validate_name_is_present_if_not_anonymous(
        cls, values: dict[str, Any]
    ) -> dict[str, Any]:
        return validate_name_present_on_nonanonymous_blocks(values)

    @model_serializer(mode="wrap")
    def serialize_data(
        self, handler: SerializerFunctionWrapHandler, info: SerializationInfo
    ) -> Any:
        self.data = visit_collection(
            self.data,
            visit_fn=partial(handle_secret_render, context=info.context or {}),
            return_data=True,
        )
        return handler(self)


class Flow(BaseModel):
    """An ORM representation of flow data."""

    id: UUID = Field(default=..., description="The ID of the flow.")
    name: Name = Field(
        default=..., description="The name of the flow", examples=["my-flow"]
    )
    tags: list[str] = Field(
        default_factory=list,
        description="A list of flow tags",
        examples=[["tag-1", "tag-2"]],
    )
    labels: KeyValueLabelsField


class DeploymentSchedule(BaseModel):
    id: UUID = Field(
        default=...,
        description="The ID of the deployment schedule.",
    )
    deployment_id: Optional[UUID] = Field(
        default=None,
        description="The deployment id associated with this schedule.",
    )
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


class Deployment(BaseModel):
    """An ORM representation of deployment data."""

    name: Name = Field(default=..., description="The name of the deployment.")
    version: Optional[str] = Field(
        default=None, description="An optional version for the deployment."
    )
    description: Optional[str] = Field(
        default=None, description="A description for the deployment."
    )
    flow_id: UUID = Field(
        default=..., description="The flow id associated with the deployment."
    )
    paused: bool = Field(
        default=False, description="Whether or not the deployment is paused."
    )
    concurrency_limit: Optional[int] = Field(
        default=None, description="The concurrency limit for the deployment."
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


class ConcurrencyLimit(BaseModel):
    """An ORM representation of a concurrency limit."""

    tag: str = Field(
        default=..., description="A tag the concurrency limit is applied to."
    )
    concurrency_limit: int = Field(default=..., description="The concurrency limit.")
    active_slots: list[UUID] = Field(
        default_factory=list,
        description="A list of active run ids using a concurrency slot",
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
    concurrency_limit: Optional[NonNegativeInteger] = Field(
        default=None, description="A concurrency limit for the work pool."
    )
    status: Optional[WorkPoolStatus] = Field(
        default=None, description="The current status of the work pool."
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
