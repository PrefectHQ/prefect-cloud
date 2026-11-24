"""
Reads a function's signature from Python source code without running the source.

`prefect-cloud deploy` builds a flow's parameter schema from a file it downloads
from GitHub, which may be someone else's code.  Running any part of that file
would run it on the deploying machine with the user's credentials, so the file
is only parsed.  Its absolute imports are resolved against installed modules,
enum and pydantic model classes are rebuilt from their field declarations, and
annotations and default values are evaluated by `SourceNamespace.evaluate`,
which permits literals, names, public attributes, subscripts, `|`, and calls to
a fixed set of value constructors.  Anything else counts as unknown: an unknown
annotation becomes `inspect.Parameter.empty`, and an unknown default becomes
`None`.
"""

import ast
import builtins
import datetime
import decimal
import enum
import importlib
import inspect
import pathlib
import uuid
from logging import Logger, getLogger
from typing import Any, Optional, cast

import pydantic

logger: Logger = getLogger(__name__)

# Calling these with literal arguments only builds a value, so a default like
# `datetime.date(2025, 1, 1)` or `Field(default=1)` can be evaluated safely.
VALUE_CONSTRUCTORS: tuple[Any, ...] = (
    bool,
    bytes,
    complex,
    dict,
    float,
    frozenset,
    int,
    list,
    set,
    str,
    tuple,
    datetime.date,
    datetime.datetime,
    datetime.time,
    datetime.timedelta,
    datetime.timezone,
    decimal.Decimal,
    pathlib.Path,
    pathlib.PurePath,
    pathlib.PurePosixPath,
    pathlib.PureWindowsPath,
    uuid.UUID,
    pydantic.Field,
)


class UnknownExpression(Exception):
    """An expression uses something `SourceNamespace.evaluate` does not permit."""


class SourceNamespace:
    """The module-level names of a source file, resolved without running it."""

    def __init__(self, module: ast.Module):
        self.names: dict[str, Any] = {}
        # Classes rebuilt from this file; calling them only validates field values.
        self.rebuilt_classes: list[type] = []
        for node in module.body:
            self._bind(node)

    def _bind(self, node: ast.stmt) -> None:
        if isinstance(node, ast.Import):
            for alias in node.names:
                self._bind_import(alias)
        elif isinstance(node, ast.ImportFrom):
            # Relative imports would need the rest of the repository, which
            # was not downloaded.
            if node.level == 0 and node.module:
                for alias in node.names:
                    self._bind_import_from(node.module, alias)
        elif isinstance(node, ast.ClassDef):
            self.names[node.name] = self._rebuild_class(node)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            # Module-level assignments are how type aliases and constants used
            # in signatures are defined, e.g. `Number = Union[int, float]`.
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names = [target.id for target in targets if isinstance(target, ast.Name)]
            try:
                value = self.evaluate(node.value)
            except UnknownExpression:
                for name in names:
                    self.names.pop(name, None)
            else:
                for name in names:
                    self.names[name] = value

    def _bind_import(self, alias: ast.alias) -> None:
        bound_name = alias.asname or alias.name.split(".")[0]
        try:
            module = importlib.import_module(alias.name)
            if not alias.asname:
                # `import a.b` binds `a`
                module = importlib.import_module(bound_name)
        except Exception as e:
            logger.debug("Failed to import %s: %s", alias.name, e)
            return
        self.names[bound_name] = module

    def _bind_import_from(self, module_name: str, alias: ast.alias) -> None:
        if alias.name == "*":
            return
        try:
            module = importlib.import_module(module_name)
            if hasattr(module, alias.name):
                value = getattr(module, alias.name)
            else:
                value = importlib.import_module(f"{module_name}.{alias.name}")
        except Exception as e:
            logger.debug("Failed to import %s from %s: %s", alias.name, module_name, e)
            return
        self.names[alias.asname or alias.name] = value

    def _rebuild_class(self, node: ast.ClassDef) -> type:
        bases = [base for base in map(self._try_evaluate, node.bases) if base]
        try:
            if enum_base := _first_subclass(bases, enum.Enum):
                rebuilt = self._rebuild_enum(node, enum_base, bases)
            elif model_base := _first_subclass(bases, pydantic.BaseModel):
                rebuilt = self._rebuild_model(node, model_base)
            else:
                return type(node.name, (), {})
        except Exception as e:
            logger.debug("Failed to rebuild class %s: %s", node.name, e)
            return type(node.name, (), {})
        self.rebuilt_classes.append(rebuilt)
        return rebuilt

    def _rebuild_enum(
        self, node: ast.ClassDef, enum_base: type[enum.Enum], bases: list[Any]
    ) -> type:
        members: dict[str, Any] = {}
        for statement in node.body:
            if (
                isinstance(statement, ast.Assign)
                and len(statement.targets) == 1
                and isinstance(statement.targets[0], ast.Name)
            ):
                value = self._try_evaluate(statement.value)
                if value is not None:
                    members[statement.targets[0].id] = value
        mixin = next(
            (
                base
                for base in bases
                if isinstance(base, type) and not issubclass(base, enum.Enum)
            ),
            None,
        )
        # Enum's functional API returns a new class, which type checkers read as
        # an instance of `enum_base`.
        return cast(type, enum_base(node.name, members, type=mixin))

    def _rebuild_model(
        self, node: ast.ClassDef, model_base: type[pydantic.BaseModel]
    ) -> type:
        fields: dict[str, Any] = {}
        for statement in node.body:
            if isinstance(statement, ast.AnnAssign) and isinstance(
                statement.target, ast.Name
            ):
                annotation = self.annotation(statement.annotation)
                if annotation is inspect.Parameter.empty:
                    annotation = Any
                if statement.value is None:
                    default = ...
                else:
                    default = self._try_evaluate(statement.value)
                fields[statement.target.id] = (annotation, default)
        return pydantic.create_model(node.name, __base__=model_base, **fields)

    def annotation(self, node: Optional[ast.expr]) -> Any:
        """Evaluate an annotation, or return `inspect.Parameter.empty`."""
        if node is None:
            return inspect.Parameter.empty
        try:
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                node = ast.parse(node.value, mode="eval").body
            return self.evaluate(node)
        except (UnknownExpression, SyntaxError) as e:
            logger.debug("Failed to evaluate annotation: %s", e)
            return inspect.Parameter.empty

    def default(self, node: Optional[ast.expr]) -> Any:
        """Evaluate a default value, or return `None` when it is unknown."""
        if node is None:
            return inspect.Parameter.empty
        return self._try_evaluate(node)

    def _try_evaluate(self, node: ast.expr) -> Any:
        try:
            return self.evaluate(node)
        except UnknownExpression as e:
            logger.debug("Failed to evaluate expression: %s", e)
            return None

    def evaluate(self, node: ast.expr) -> Any:
        """Evaluate an expression from the source using only permitted operations."""
        if isinstance(node, ast.Constant):
            return node.value

        if isinstance(node, ast.Name):
            if node.id in self.names:
                return self.names[node.id]
            if hasattr(builtins, node.id):
                return getattr(builtins, node.id)
            raise UnknownExpression(f"unknown name {node.id!r}")

        if isinstance(node, ast.Attribute):
            # Private and dunder attributes lead to interpreter internals,
            # like `().__class__.__base__.__subclasses__()`.
            if node.attr.startswith("_"):
                raise UnknownExpression(f"private attribute {node.attr!r}")
            owner = self.evaluate(node.value)
            return self._apply(getattr, owner, node.attr)

        if isinstance(node, ast.Subscript):
            container = self.evaluate(node.value)
            key = self.evaluate(node.slice)
            return self._apply(lambda: container[key])

        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            left, right = self.evaluate(node.left), self.evaluate(node.right)
            return self._apply(lambda: left | right)

        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
            operand = self.evaluate(node.operand)
            if not isinstance(operand, (int, float, complex)) or isinstance(
                operand, bool
            ):
                raise UnknownExpression("unary operator on a non-number")
            return -operand if isinstance(node.op, ast.USub) else operand

        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            items = [self.evaluate(element) for element in node.elts]
            if isinstance(node, ast.Tuple):
                return tuple(items)
            return items if isinstance(node, ast.List) else self._apply(set, items)

        if isinstance(node, ast.Dict):
            if any(key is None for key in node.keys):
                raise UnknownExpression("dictionary unpacking")
            return self._apply(
                dict,
                [
                    (self.evaluate(key), self.evaluate(value))
                    for key, value in zip(node.keys, node.values)
                    if key is not None
                ],
            )

        if isinstance(node, ast.Call):
            function = self.evaluate(node.func)
            if not self._is_value_constructor(function):
                raise UnknownExpression(f"call to {function!r}")
            if any(isinstance(arg, ast.Starred) for arg in node.args) or any(
                keyword.arg is None for keyword in node.keywords
            ):
                raise UnknownExpression("argument unpacking")
            args = [self.evaluate(arg) for arg in node.args]
            kwargs = {
                keyword.arg: self.evaluate(keyword.value)
                for keyword in node.keywords
                if keyword.arg is not None
            }
            return self._apply(function, *args, **kwargs)

        raise UnknownExpression(f"unsupported expression {type(node).__name__}")

    def _is_value_constructor(self, function: Any) -> bool:
        return any(function is allowed for allowed in VALUE_CONSTRUCTORS) or any(
            function is rebuilt for rebuilt in self.rebuilt_classes
        )

    @staticmethod
    def _apply(function: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return function(*args, **kwargs)
        except Exception as e:
            raise UnknownExpression(str(e)) from e


def _first_subclass(bases: list[Any], parent: type) -> Optional[Any]:
    return next(
        (base for base in bases if isinstance(base, type) and issubclass(base, parent)),
        None,
    )


def signature_from_source(source_code: str, func_name: str) -> inspect.Signature:
    """Read the signature of the function named `func_name` in `source_code`."""
    module = ast.parse(source_code)
    func_def = next(
        (
            node
            for node in ast.walk(module)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == func_name
        ),
        None,
    )
    if func_def is None:
        raise ValueError(f"Function {func_name} not found in source code")

    namespace = SourceNamespace(module)
    arguments = func_def.args
    parameters: list[inspect.Parameter] = []

    positional = arguments.posonlyargs + arguments.args
    positional_defaults: list[Optional[ast.expr]] = [None] * (
        len(positional) - len(arguments.defaults)
    ) + list(arguments.defaults)
    for index, (arg, default) in enumerate(zip(positional, positional_defaults)):
        kind = (
            inspect.Parameter.POSITIONAL_ONLY
            if index < len(arguments.posonlyargs)
            else inspect.Parameter.POSITIONAL_OR_KEYWORD
        )
        parameters.append(
            inspect.Parameter(
                arg.arg,
                kind,
                annotation=namespace.annotation(arg.annotation),
                default=namespace.default(default),
            )
        )

    if arguments.vararg:
        parameters.append(
            inspect.Parameter(arguments.vararg.arg, inspect.Parameter.VAR_POSITIONAL)
        )

    for arg, default in zip(arguments.kwonlyargs, arguments.kw_defaults):
        parameters.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.KEYWORD_ONLY,
                annotation=namespace.annotation(arg.annotation),
                default=namespace.default(default),
            )
        )

    if arguments.kwarg:
        parameters.append(
            inspect.Parameter(arguments.kwarg.arg, inspect.Parameter.VAR_KEYWORD)
        )

    return inspect.Signature(
        parameters, return_annotation=namespace.annotation(func_def.returns)
    )
