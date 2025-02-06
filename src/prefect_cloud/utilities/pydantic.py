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


def get_class_fields_only(model: type[BaseModel]) -> set[str]:
    """
    Gets all the field names defined on the model class but not any parent classes.
    Any fields that are on the parent but redefined on the subclass are included.
    """
    subclass_class_fields = set(model.__annotations__.keys())
    parent_class_fields: set[str] = set()

    for base in model.__class__.__bases__:
        if issubclass(base, BaseModel):
            parent_class_fields.update(base.__annotations__.keys())

    return (subclass_class_fields - parent_class_fields) | (
        subclass_class_fields & parent_class_fields
    )
