"""
Utilities for working with Python callables.
"""

import ast
import inspect

from functools import partial
from logging import Logger
from typing import Any, Callable, Optional
from pydantic import (
    BaseModel,
    ConfigDict,
    create_model,
    TypeAdapter,
    PydanticUndefinedAnnotation,
)
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaValue
from pydantic_core import core_schema
import pydantic
from prefect_cloud.utilities.source_signature import signature_from_source
from griffe import Docstring, DocstringSectionKind, Parser, parse
from typing_extensions import Literal
from logging import getLogger
from contextlib import contextmanager


logger: Logger = getLogger(__name__)


@contextmanager
def disable_logger(name: str):
    """
    Get a logger by name and disables it within the context manager.
    Upon exiting the context manager, the logger is returned to its
    original state.
    """
    logger = getLogger(name=name)

    # determine if it's already disabled
    base_state = logger.disabled
    try:
        # disable the logger
        logger.disabled = True
        yield
    finally:
        # return to base state
        logger.disabled = base_state


class GenerateEmptySchemaForUserClasses(GenerateJsonSchema):
    """
    This custom schema overrides the default pydantic is-instance schema
    behavior to simply return an empty dict for user-defined classes
    """

    def is_instance_schema(
        self, schema: core_schema.IsInstanceSchema
    ) -> JsonSchemaValue:
        return {}


class ParameterSchema(pydantic.BaseModel):
    """Simple data model corresponding to an OpenAPI `Schema`."""

    title: Literal["Parameters"] = "Parameters"
    type: Literal["object"] = "object"
    properties: dict[str, Any] = pydantic.Field(default_factory=dict)
    required: list[str] = pydantic.Field(default_factory=list)
    definitions: dict[str, Any] = pydantic.Field(default_factory=dict)

    def model_dump_for_openapi(self) -> dict[str, Any]:
        result = self.model_dump(mode="python", exclude_none=True)
        if "required" in result and not result["required"]:
            del result["required"]
        return result


def parameter_docstrings(docstring: Optional[str]) -> dict[str, str]:
    """
    Given a docstring in Google docstring format, parse the parameter section
    and return a dictionary that maps parameter names to docstring.

    Args:
        docstring: The function's docstring.

    Returns:
        Mapping from parameter names to docstrings.
    """
    param_docstrings: dict[str, str] = {}

    if not docstring:
        return param_docstrings

    with disable_logger("griffe"):
        parsed = parse(Docstring(docstring), Parser.google)
        for section in parsed:
            if section.kind != DocstringSectionKind.parameters:
                continue
            param_docstrings = {
                parameter.name: parameter.description for parameter in section.value
            }

    return param_docstrings


def process_v2_params(
    param: inspect.Parameter,
    *,
    position: int,
    docstrings: dict[str, str],
    aliases: dict[str, str],
) -> tuple[str, Any, Any]:
    """
    Generate a sanitized name, type, and pydantic.Field for a given parameter.

    This implementation is exactly the same as the v1 implementation except
    that it uses pydantic v2 constructs.
    """
    # Pydantic model creation will fail if names collide with the BaseModel type
    if hasattr(pydantic.BaseModel, param.name):
        name = param.name + "__"
        aliases[name] = param.name
    else:
        name = param.name

    type_ = Any if param.annotation is inspect.Parameter.empty else param.annotation

    field = pydantic.Field(
        default=... if param.default is param.empty else param.default,
        title=param.name,
        description=docstrings.get(param.name, None),
        alias=aliases.get(name),
        json_schema_extra={"position": position},
    )
    return name, type_, field


def create_v2_schema(
    name_: str,
    model_cfg: Optional[ConfigDict] = None,
    model_base: Optional[type[BaseModel]] = None,
    model_fields: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Create a pydantic v2 model and craft a v1 compatible schema from it.
    """
    model_fields = model_fields or {}
    model = create_model(
        name_, __config__=model_cfg, __base__=model_base, **model_fields
    )
    try:
        adapter = TypeAdapter(model)
    except PydanticUndefinedAnnotation as exc:
        # in v1 this raises a TypeError, which is handled by parameter_schema
        raise TypeError(exc.message)

    # root model references under #definitions
    schema = adapter.json_schema(
        by_alias=True,
        ref_template="#/definitions/{model}",
        schema_generator=GenerateEmptySchemaForUserClasses,
    )
    # ensure backwards compatibility by copying $defs into definitions
    if "$defs" in schema:
        schema["definitions"] = schema["$defs"]
    return schema


def parameter_schema(fn: Callable[..., Any]) -> ParameterSchema:
    """Given a function, generates an OpenAPI-compatible description
    of the function's arguments, including:
        - name
        - typing information
        - whether it is required
        - a default value
        - additional constraints (like possible enum values)

    Args:
        fn (Callable): The function whose arguments will be serialized

    Returns:
        ParameterSchema: the argument schema
    """
    try:
        signature = inspect.signature(fn, eval_str=True)  # novm
    except (NameError, TypeError):
        # `eval_str` is not available in Python < 3.10
        signature = inspect.signature(fn)

    docstrings = parameter_docstrings(inspect.getdoc(fn))

    return generate_parameter_schema(signature, docstrings)


def generate_parameter_schema(
    signature: inspect.Signature, docstrings: dict[str, str]
) -> ParameterSchema:
    """
    Generate a parameter schema from a function signature and docstrings.

    To get a signature from a function, use `inspect.signature(fn)` or
    `signature_from_source(source_code, func_name)`.

    Args:
        signature: The function signature.
        docstrings: A dictionary mapping parameter names to docstrings.

    Returns:
        ParameterSchema: The parameter schema.
    """

    model_fields: dict[str, Any] = {}
    aliases: dict[str, str] = {}

    config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    create_schema = partial(create_v2_schema, model_cfg=config)
    process_params = process_v2_params

    for position, param in enumerate(signature.parameters.values()):
        name, type_, field = process_params(
            param, position=position, docstrings=docstrings, aliases=aliases
        )
        # Generate a Pydantic model at each step so we can check if this parameter
        # type supports schema generation
        try:
            create_schema("CheckParameter", model_fields={name: (type_, field)})
        except (ValueError, TypeError):
            # This field's type is not valid for schema creation, update it to `Any`
            type_ = Any
        model_fields[name] = (type_, field)

    # Generate the final model and schema
    schema = create_schema("Parameters", model_fields=model_fields)
    return ParameterSchema(**schema)


def _get_docstring_from_source(source_code: str, func_name: str) -> Optional[str]:
    """
    Extract the docstring of a function from its source code.

    Args:
        source_code (str): The source code of the function.
        func_name (str): The name of the function.

    Returns:
        The docstring of the function. If the function has no docstring, returns None.
    """
    parsed_code = ast.parse(source_code)

    func_def = next(
        (
            node
            for node in ast.walk(parsed_code)
            if isinstance(
                node,
                (
                    ast.FunctionDef,
                    ast.AsyncFunctionDef,
                ),
            )
            and node.name == func_name
        ),
        None,
    )
    if func_def is None:
        raise ValueError(f"Function {func_name} not found in source code")

    if (
        func_def.body
        and isinstance(func_def.body[0], ast.Expr)
        and isinstance(func_def.body[0].value, ast.Constant)
        and isinstance(func_def.body[0].value.value, str)
    ):
        return func_def.body[0].value.value
    return None


def get_parameter_schema_from_content(
    content: str, function_name: str
) -> ParameterSchema:
    signature = signature_from_source(content, function_name)
    docstring = _get_docstring_from_source(content, function_name)
    return generate_parameter_schema(signature, parameter_docstrings(docstring))
