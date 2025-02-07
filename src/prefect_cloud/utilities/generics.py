from typing import Any, TypeVar, cast
from enum import Enum, auto
from pydantic import BaseModel, Secret
from pydantic_core import SchemaValidator, core_schema
from prefect.utilities.names import obfuscate

T = TypeVar("T", bound=BaseModel)

ListValidator: SchemaValidator = SchemaValidator(
    schema=core_schema.list_schema(
        items_schema=core_schema.dict_schema(
            keys_schema=core_schema.str_schema(), values_schema=core_schema.any_schema()
        )
    )
)


def validate_list(model: type[T], input: Any) -> list[T]:
    return [model.model_validate(item) for item in ListValidator.validate_python(input)]


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


class AutoEnum(str, Enum):
    """
    An enum class that automatically generates value from variable names.

    This guards against common errors where variable names are updated but values are
    not.

    In addition, because AutoEnums inherit from `str`, they are automatically
    JSON-serializable.

    See https://docs.python.org/3/library/enum.html#using-automatic-values

    Example:
        ```python
        class MyEnum(AutoEnum):
            RED = AutoEnum.auto() # equivalent to RED = 'RED'
            BLUE = AutoEnum.auto() # equivalent to BLUE = 'BLUE'
        ```
    """

    @staticmethod
    def _generate_next_value_(name: str, *_: object, **__: object) -> str:
        return name

    @staticmethod
    def auto() -> str:
        """
        Exposes `enum.auto()` to avoid requiring a second import to use `AutoEnum`
        """
        return auto()

    def __repr__(self) -> str:
        return f"{type(self).__name__}.{self.value}"
