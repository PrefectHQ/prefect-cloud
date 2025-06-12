"""
Utilities for working with Python callables.
"""

import ast
import importlib.util
import inspect

import sys
from importlib.machinery import ModuleSpec
import os
from functools import partial
from types import ModuleType
from logging import Logger
from pathlib import Path
from typing import Any, Callable, Optional, TYPE_CHECKING
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


def parameter_schema_from_entrypoint(entrypoint: str) -> ParameterSchema:
    """
    Generate a parameter schema from an entrypoint string.

    Will load the source code of the function and extract the signature and docstring
    to generate the schema.

    Useful for generating a schema for a function when instantiating the function may
    not be possible due to missing imports or other issues.

    Args:
        entrypoint: A string representing the entrypoint to a function. The string
            should be in the format of `module.path.to.function:do_stuff`.

    Returns:
        ParameterSchema: The parameter schema for the function.
    """
    filepath = None
    if ":" in entrypoint:
        # split by the last colon once to handle Windows paths with drive letters i.e C:\path\to\file.py:do_stuff
        path, func_name = entrypoint.rsplit(":", maxsplit=1)
        source_code = Path(path).read_text()
        filepath = path
    else:
        path, func_name = entrypoint.rsplit(".", maxsplit=1)
        spec = importlib.util.find_spec(path)
        if not spec or not spec.origin:
            raise ValueError(f"Could not find module {path!r}")
        source_code = Path(spec.origin).read_text()
    signature = _generate_signature_from_source(source_code, func_name, filepath)
    docstring = _get_docstring_from_source(source_code, func_name)
    return generate_parameter_schema(signature, parameter_docstrings(docstring))


def generate_parameter_schema(
    signature: inspect.Signature, docstrings: dict[str, str]
) -> ParameterSchema:
    """
    Generate a parameter schema from a function signature and docstrings.

    To get a signature from a function, use `inspect.signature(fn)` or
    `_generate_signature_from_source(source_code, func_name)`.

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


def _generate_signature_from_source(
    source_code: str, func_name: str, filepath: Optional[str] = None
) -> inspect.Signature:
    """
    Extract the signature of a function from its source code.

    Will ignore missing imports and exceptions while loading local class definitions.

    Args:
        source_code: The source code where the function named `func_name` is declared.
        func_name: The name of the function.

    Returns:
        The signature of the function.
    """
    # Load the namespace from the source code. Missing imports and exceptions while
    # loading local class definitions are ignored.
    namespace = safe_load_namespace(source_code, filepath=filepath)
    # Parse the source code into an AST
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
    parameters: list[inspect.Parameter] = []
    annotation: Any = None
    # Handle annotations for positional only args e.g. def func(a, /, b, c)
    for arg in func_def.args.posonlyargs:
        name = arg.arg
        annotation = arg.annotation
        if annotation is not None:
            try:
                ann_code = compile(ast.Expression(annotation), "<string>", "eval")
                annotation = eval(ann_code, namespace)
            except Exception as e:
                logger.debug("Failed to evaluate annotation for %s: %s", name, e)
                annotation = inspect.Parameter.empty
        else:
            annotation = inspect.Parameter.empty

        param = inspect.Parameter(
            name, inspect.Parameter.POSITIONAL_ONLY, annotation=annotation
        )
        parameters.append(param)

    # Determine the annotations for args e.g. def func(a: int, b: str, c: float)
    for arg in func_def.args.args:
        name = arg.arg
        annotation = arg.annotation
        if annotation is not None:
            try:
                # Compile and evaluate the annotation
                ann_code = compile(ast.Expression(annotation), "<string>", "eval")
                annotation = eval(ann_code, namespace)
            except Exception as e:
                # Don't raise an error if the annotation evaluation fails. Set the
                # annotation to `inspect.Parameter.empty` instead which is equivalent to
                # not having an annotation.
                logger.debug("Failed to evaluate annotation for %s: %s", name, e)
                annotation = inspect.Parameter.empty
        else:
            annotation = inspect.Parameter.empty

        param = inspect.Parameter(
            name, inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
        )
        parameters.append(param)

    # Handle default values for args e.g. def func(a=1, b="hello", c=3.14)
    num_defaults = len(func_def.args.defaults)
    num_args = len(func_def.args.args)
    defaults: list[Any] = [None] * (num_args - num_defaults) + list(
        func_def.args.defaults
    )

    for param, default in zip(parameters, defaults):
        if default is not None:
            try:
                def_code = compile(ast.Expression(default), "<string>", "eval")
                default = eval(def_code, namespace)
            except Exception as e:
                logger.debug(
                    "Failed to evaluate default value for %s: %s", param.name, e
                )
                default = None  # Set to None if evaluation fails
            parameters[parameters.index(param)] = param.replace(default=default)

    # Handle annotations for keyword only args e.g. def func(*, a: int, b: str)
    for kwarg in func_def.args.kwonlyargs:
        name = kwarg.arg
        annotation = kwarg.annotation
        if annotation is not None:
            try:
                ann_code = compile(ast.Expression(annotation), "<string>", "eval")
                annotation = eval(ann_code, namespace)
            except Exception as e:
                logger.debug("Failed to evaluate annotation for %s: %s", name, e)
                annotation = inspect.Parameter.empty
        else:
            annotation = inspect.Parameter.empty

        param = inspect.Parameter(
            name, inspect.Parameter.KEYWORD_ONLY, annotation=annotation
        )
        parameters.append(param)

    # Handle default values for keyword only args e.g. def func(*, a=1, b="hello")
    num_kw_defaults = len(func_def.args.kw_defaults)
    num_kwonlyargs = len(func_def.args.kwonlyargs)
    kw_defaults: list[Any] = [None] * (num_kwonlyargs - num_kw_defaults) + list(
        func_def.args.kw_defaults
    )

    for param, default in zip(parameters[-num_kwonlyargs:], kw_defaults):
        if default is not None:
            try:
                def_code = compile(ast.Expression(default), "<string>", "eval")
                default = eval(def_code, namespace)
            except Exception as e:
                logger.debug(
                    "Failed to evaluate default value for %s: %s", param.name, e
                )
                default = None
            parameters[parameters.index(param)] = param.replace(default=default)

    # Handle annotations for varargs and kwargs e.g. def func(*args: int, **kwargs: str)
    if func_def.args.vararg:
        parameters.append(
            inspect.Parameter(
                func_def.args.vararg.arg, inspect.Parameter.VAR_POSITIONAL
            )
        )
    if func_def.args.kwarg:
        parameters.append(
            inspect.Parameter(func_def.args.kwarg.arg, inspect.Parameter.VAR_KEYWORD)
        )

    # Handle return annotation e.g. def func() -> int
    return_annotation: Any = func_def.returns
    if return_annotation is not None:
        try:
            ret_ann_code = compile(
                ast.Expression(return_annotation), "<string>", "eval"
            )
            return_annotation = eval(ret_ann_code, namespace)
        except Exception as e:
            logger.debug("Failed to evaluate return annotation: %s", e)
            return_annotation = inspect.Signature.empty

    return inspect.Signature(parameters, return_annotation=return_annotation)


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


def safe_load_namespace(
    source_code: str, filepath: Optional[str] = None
) -> dict[str, Any]:
    """
    Safely load a namespace from source code, optionally handling relative imports.

    If a `filepath` is provided, `sys.path` is modified to support relative imports.
    Changes to `sys.path` are reverted after completion, but this function is not thread safe
    and use of it in threaded contexts may result in undesirable behavior.

    Args:
        source_code: The source code to load
        filepath: Optional file path of the source code. If provided, enables relative imports.

    Returns:
        The namespace loaded from the source code.
    """
    parsed_code = ast.parse(source_code)

    namespace: dict[str, Any] = {"__name__": "prefect_safe_namespace_loader"}

    # Remove the body of the if __name__ == "__main__": block
    new_body = [node for node in parsed_code.body if not _is_main_block(node)]
    parsed_code.body = new_body

    temp_module = None
    original_sys_path = None

    if filepath:
        # Setup for relative imports
        file_dir = os.path.dirname(os.path.abspath(filepath))
        package_name = os.path.basename(file_dir)
        parent_dir = os.path.dirname(file_dir)

        # Save original sys.path and modify it
        original_sys_path = sys.path.copy()
        sys.path.insert(0, parent_dir)
        sys.path.insert(0, file_dir)

        # Create a temporary module for import context
        temp_module = ModuleType(package_name)
        temp_module.__file__ = filepath
        temp_module.__package__ = package_name

        # Create a spec for the module
        temp_module.__spec__ = ModuleSpec(package_name, None)
        temp_module.__spec__.loader = None
        temp_module.__spec__.submodule_search_locations = [file_dir]

    try:
        for node in parsed_code.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module_name = alias.name
                    as_name = alias.asname or module_name
                    try:
                        namespace[as_name] = importlib.import_module(module_name)
                        logger.debug("Successfully imported %s", module_name)
                    except ImportError as e:
                        logger.debug(f"Failed to import {module_name}: {e}")
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module or ""
                if filepath:
                    try:
                        if node.level > 0:
                            # For relative imports, use the parent package to inform the import
                            if TYPE_CHECKING:
                                assert temp_module is not None
                                assert temp_module.__package__ is not None
                            package_parts = temp_module.__package__.split(".")
                            if len(package_parts) < node.level:
                                raise ImportError(
                                    "Attempted relative import beyond top-level package"
                                )
                            parent_package = ".".join(
                                package_parts[: (1 - node.level)]
                                if node.level > 1
                                else package_parts
                            )
                            module = importlib.import_module(
                                f".{module_name}" if module_name else "",
                                package=parent_package,
                            )
                        else:
                            # Absolute imports are handled as normal
                            module = importlib.import_module(module_name)

                        for alias in node.names:
                            name = alias.name
                            asname = alias.asname or name
                            if name == "*":
                                # Handle 'from module import *'
                                module_dict = {
                                    k: v
                                    for k, v in module.__dict__.items()
                                    if not k.startswith("_")
                                }
                                namespace.update(module_dict)
                            else:
                                try:
                                    attribute = getattr(module, name)
                                    namespace[asname] = attribute
                                except AttributeError as e:
                                    logger.debug(
                                        "Failed to retrieve %s from %s: %s",
                                        name,
                                        module_name,
                                        e,
                                    )
                    except ImportError as e:
                        logger.debug("Failed to import from %s: %s", module_name, e)
                else:
                    # Handle as absolute import when no filepath is provided
                    try:
                        module = importlib.import_module(module_name)
                        for alias in node.names:
                            name = alias.name
                            asname = alias.asname or name
                            if name == "*":
                                # Handle 'from module import *'
                                module_dict = {
                                    k: v
                                    for k, v in module.__dict__.items()
                                    if not k.startswith("_")
                                }
                                namespace.update(module_dict)
                            else:
                                try:
                                    attribute = getattr(module, name)
                                    namespace[asname] = attribute
                                except AttributeError as e:
                                    logger.debug(
                                        "Failed to retrieve %s from %s: %s",
                                        name,
                                        module_name,
                                        e,
                                    )
                    except ImportError as e:
                        logger.debug("Failed to import from %s: %s", module_name, e)
        # Handle local definitions
        for node in parsed_code.body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.Assign)):
                try:
                    code = compile(
                        ast.Module(body=[node], type_ignores=[]),
                        filename="<ast>",
                        mode="exec",
                    )
                    exec(code, namespace)
                except Exception as e:
                    logger.debug("Failed to compile: %s", e)

    finally:
        # Restore original sys.path if it was modified
        if original_sys_path:
            sys.path[:] = original_sys_path

    return namespace


def _is_main_block(node: ast.AST):
    """
    Check if the node is an `if __name__ == "__main__":` block.
    """
    if isinstance(node, ast.If):
        try:
            # Check if the condition is `if __name__ == "__main__":`
            if (
                isinstance(node.test, ast.Compare)
                and isinstance(node.test.left, ast.Name)
                and node.test.left.id == "__name__"
                and isinstance(node.test.comparators[0], ast.Constant)
                and node.test.comparators[0].value == "__main__"
            ):
                return True
        except AttributeError:
            pass
    return False


def get_parameter_schema_from_content(
    content: str, function_name: str
) -> ParameterSchema:
    signature = _generate_signature_from_source(content, function_name)
    docstring = _get_docstring_from_source(content, function_name)
    return generate_parameter_schema(signature, parameter_docstrings(docstring))
