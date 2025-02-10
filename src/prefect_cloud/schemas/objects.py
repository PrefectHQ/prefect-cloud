from __future__ import annotations

import re
import datetime
from typing import Any, Optional, Union, TypeAlias
from uuid import UUID, uuid4
from pydantic import (
    Field,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    BaseModel,
)
from prefect_cloud.utilities.generics import handle_secret_render


from prefect_cloud.types import Name


def validate_block_document_name(value: Optional[str]) -> Optional[str]:
    field_name = "Block document name"
    if value is not None and not re.match("^[a-z0-9-]*$", value):
        raise ValueError(
            f"{field_name} must only contain lowercase letters, numbers, and"
            " underscores."
        )
    return value


class DeploymentFlowRun(BaseModel):
    name: str = Field(default=..., examples=["my-flow-run"])
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
        if self.data.get("secret"):
            self.data["secret"] = handle_secret_render(
                self.data["secret"], info.context or {}
            )
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
    schedule: SCHEDULE_TYPES = Field(
        default=..., description="The schedule for the deployment."
    )
    active: bool = Field(
        default=True, description="Whether or not the schedule is active."
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


class CronSchedule(BaseModel):
    """
    Cron schedule
    """

    cron: str = Field(default=..., examples=["0 0 * * *"])
    timezone: Optional[str] = Field(default=None, examples=["America/New_York"])

    @field_validator("cron")
    @classmethod
    def valid_cron_string(cls, v: str) -> str:
        from croniter import croniter

        # croniter allows "random" and "hashed" expressions
        # which we do not support https://github.com/kiorky/croniter
        if not croniter.is_valid(v):
            raise ValueError(f'Invalid cron string: "{v}"')
        elif any(c for c in v.split() if c.casefold() in ["R", "H", "r", "h"]):
            raise ValueError(
                f'Random and Hashed expressions are unsupported, received: "{v}"'
            )
        return v


class IntervalSchedule(BaseModel):
    interval: datetime.timedelta = Field(gt=datetime.timedelta(0))


class RRuleSchedule(BaseModel):
    rrule: str


SCHEDULE_TYPES: TypeAlias = Union[IntervalSchedule, CronSchedule, RRuleSchedule]
