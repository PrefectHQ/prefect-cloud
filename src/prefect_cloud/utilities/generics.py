from typing import Any, TypeVar, cast
from pydantic import BaseModel, Secret
from pydantic_core import SchemaValidator, core_schema


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


OBFUSCATED_PREFIX = "****"


def obfuscate(s: Any, show_tail: bool = False) -> str:
    """
    Obfuscates any data type's string representation. See `obfuscate_string`.
    """
    if s is None:
        return OBFUSCATED_PREFIX + "*" * 4

    return obfuscate_string(str(s), show_tail=show_tail)


def obfuscate_string(s: str, show_tail: bool = False) -> str:
    """
    Obfuscates a string by returning a new string of 8 characters. If the input
    string is longer than 10 characters and show_tail is True, then up to 4 of
    its final characters will become final characters of the obfuscated string;
    all other characters are "*".

    "abc"      -> "********"
    "abcdefgh" -> "********"
    "abcdefghijk" -> "*******k"
    "abcdefghijklmnopqrs" -> "****pqrs"
    """
    result = OBFUSCATED_PREFIX + "*" * 4
    # take up to 4 characters, but only after the 10th character
    suffix = s[10:][-4:]
    if suffix and show_tail:
        result = f"{result[: -len(suffix)]}{suffix}"
    return result


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
