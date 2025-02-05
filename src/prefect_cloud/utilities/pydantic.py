from typing import (
    Any,
    TypeVar,
    cast,
)

from pydantic import (
    BaseModel,
    Secret,
)

from prefect.utilities.names import obfuscate

D = TypeVar("D", bound=Any)
M = TypeVar("M", bound=BaseModel)
T = TypeVar("T", bound=Any)


def handle_secret_render(value: object, context: dict[str, Any]) -> object:
    if hasattr(value, "get_secret_value"):
        return (
            cast(Secret[object], value).get_secret_value()
            if context.get("include_secrets", False)
            else obfuscate(value)
        )
    elif isinstance(value, BaseModel):
        return value.model_dump(context=context)
    return value
