"""
Reads a function's signature from Python source code without running the source.

`prefect-cloud deploy` builds a flow's parameter schema from a file it downloads
from GitHub, which may be someone else's code.  Running any part of that file
would run it on the deploying machine with the user's credentials, so the file
is only parsed.

Its absolute imports are resolved against installed modules.  Importing a
module runs that module's top-level code, which is code the user installed, not
code from the file.  From installed modules, the file may only reach modules,
classes, typing constructs, enum members, and the value constructors in
`VALUE_CONSTRUCTORS`.  Other installed values, like `os.environ` or a settings
object, are unknown, so they cannot end up in the uploaded schema.

Enum and pydantic model classes are rebuilt from their declarations, using only
the standard enum and pydantic base classes.  Annotations and default values
are evaluated by `SourceNamespace.evaluate`, which permits literals, names,
public attributes, subscripts, `|`, arithmetic on numbers, and calls to value
constructors.  Anything else counts as unknown: an unknown annotation becomes
`inspect.Parameter.empty`, and an unknown default becomes `None`.
"""

import ast
import builtins
import datetime
import decimal
import enum
import importlib
import inspect
import operator
import pathlib
import sys
import types
import uuid
from logging import Logger, getLogger
import typing
from typing import Annotated, Any, Literal, Optional, cast

import pydantic

logger: Logger = getLogger(__name__)

# Calling these with literal arguments only builds a value, so a default like
# `datetime.date(2025, 1, 1)` or `Field(default=1)` can be evaluated safely.
# `bytes` is left out because `bytes(n)` allocates n bytes.
VALUE_CONSTRUCTORS: tuple[Any, ...] = (
    bool,
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
    enum.auto,
    decimal.Decimal,
    pathlib.Path,
    pathlib.PurePath,
    pathlib.PurePosixPath,
    pathlib.PureWindowsPath,
    uuid.UUID,
    pydantic.Field,
)

# Enums are rebuilt only from these bases, because any other base class's code
# would run on member values from the file.
ENUM_BASES: tuple[type, ...] = (enum.Enum, enum.IntEnum, enum.Flag, enum.IntFlag) + (
    (enum.StrEnum,) if sys.version_info >= (3, 11) else ()
)
ENUM_MIXINS: tuple[type, ...] = (str, int, float)

ARITHMETIC: dict[type[ast.operator], Any] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}

# Repeated multiplication doubles an integer's size with each line of source, so
# integers and decimals past these bounds count as unknown rather than taking
# unbounded time and memory.
MAX_INTEGER_BITS = 128
MAX_DECIMAL_EXPONENT = 1000


class UnparsableSource(Exception):
    """The source is not Python that this interpreter can parse."""


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
        if not self._is_reference(value):
            logger.debug("Not importing value %s from %s", alias.name, module_name)
            return
        self.names[alias.asname or alias.name] = value

    def _rebuild_class(self, node: ast.ClassDef) -> type:
        bases = [base for base in map(self._try_evaluate, node.bases) if base]
        try:
            if any(base in ENUM_BASES for base in bases):
                rebuilt = self._rebuild_enum(node, bases)
            elif model_base := self._model_base(bases):
                rebuilt = self._rebuild_model(node, model_base)
            else:
                return type(node.name, (), {})
        except Exception as e:
            logger.debug("Failed to rebuild class %s: %s", node.name, e)
            return type(node.name, (), {})
        self.rebuilt_classes.append(rebuilt)
        return rebuilt

    def _rebuild_enum(self, node: ast.ClassDef, bases: list[Any]) -> type:
        if not all(base in ENUM_BASES or base in ENUM_MIXINS for base in bases):
            raise UnknownExpression("enum with a base outside the standard library")
        enum_base: type[enum.Enum] = next(base for base in bases if base in ENUM_BASES)
        mixin = next((base for base in bases if base in ENUM_MIXINS), None)
        members: list[tuple[str, Any]] = []
        for statement in node.body:
            if (
                isinstance(statement, ast.Assign)
                and len(statement.targets) == 1
                and isinstance(statement.targets[0], ast.Name)
                # Enum reads underscored names like `_missing_` and `__new__` as
                # hooks that it calls, not as members.
                and not statement.targets[0].id.startswith("_")
            ):
                try:
                    value = self.evaluate(statement.value)
                except UnknownExpression:
                    continue
                members.append((statement.targets[0].id, value))
        # Enum's functional API returns a new class, which type checkers read as
        # an instance of `enum_base`.
        return cast(type, enum_base(node.name, members, type=mixin))

    def _model_base(self, bases: list[Any]) -> Optional[type[pydantic.BaseModel]]:
        # Subclasses like pydantic-settings' `BaseSettings` read the environment
        # and files when they are built, so only plain models are rebuilt.
        return next(
            (
                base
                for base in bases
                if base is pydantic.BaseModel or base in self.rebuilt_classes
            ),
            None,
        )

    def _rebuild_model(
        self, node: ast.ClassDef, model_base: type[pydantic.BaseModel]
    ) -> type:
        fields: dict[str, Any] = {}
        for statement in node.body:
            if (
                isinstance(statement, ast.AnnAssign)
                and isinstance(statement.target, ast.Name)
                # Pydantic treats underscored names as private attributes, and
                # `create_model` reads dunder keywords like `__validators__` as
                # options, not fields.
                and not statement.target.id.startswith("_")
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
            annotation = self.evaluate(node)
            if _contains_forward_reference(annotation):
                raise UnknownExpression("annotation with a forward reference")
            return annotation
        except (UnknownExpression, SyntaxError, ValueError, RecursionError) as e:
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
        try:
            return self._evaluate(node)
        except RecursionError as e:
            raise UnknownExpression("expression nested too deeply") from e

    def _evaluate(self, node: ast.expr) -> Any:
        if isinstance(node, ast.Constant):
            return node.value

        if isinstance(node, ast.Name):
            if node.id in self.names:
                return self.names[node.id]
            if hasattr(builtins, node.id):
                return self._reference(getattr(builtins, node.id))
            raise UnknownExpression(f"unknown name {node.id!r}")

        if isinstance(node, ast.Attribute):
            # Private and dunder attributes lead to interpreter internals,
            # like `().__class__.__base__.__subclasses__()`.
            if node.attr.startswith("_"):
                raise UnknownExpression(f"private attribute {node.attr!r}")
            owner = self._evaluate(node.value)
            return self._reference(self._apply(getattr, owner, node.attr))

        if isinstance(node, ast.Subscript):
            container = self._evaluate(node.value)
            key = self._evaluate(node.slice)
            return self._apply(lambda: container[key])

        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            left, right = self._evaluate(node.left), self._evaluate(node.right)
            return self._apply(lambda: left | right)

        if isinstance(node, ast.BinOp) and type(node.op) in ARITHMETIC:
            left, right = self._evaluate(node.left), self._evaluate(node.right)
            if not (_is_number(left) and _is_number(right)):
                raise UnknownExpression("arithmetic on a non-number")
            return _bounded(self._apply(ARITHMETIC[type(node.op)], left, right))

        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
            operand = self._evaluate(node.operand)
            if not _is_number(operand):
                raise UnknownExpression("unary operator on a non-number")
            return -operand if isinstance(node.op, ast.USub) else operand

        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            items = [self._evaluate(element) for element in node.elts]
            if isinstance(node, ast.Tuple):
                return tuple(items)
            return items if isinstance(node, ast.List) else self._apply(set, items)

        if isinstance(node, ast.Dict):
            if any(key is None for key in node.keys):
                raise UnknownExpression("dictionary unpacking")
            return self._apply(
                dict,
                [
                    (self._evaluate(key), self._evaluate(value))
                    for key, value in zip(node.keys, node.values)
                    if key is not None
                ],
            )

        if isinstance(node, ast.Call):
            function = self._evaluate(node.func)
            if not self._is_value_constructor(function):
                raise UnknownExpression(
                    f"{function!r} is not a permitted value constructor"
                )
            if any(isinstance(arg, ast.Starred) for arg in node.args) or any(
                keyword.arg is None for keyword in node.keywords
            ):
                raise UnknownExpression("argument unpacking")
            args = [self._evaluate(arg) for arg in node.args]
            kwargs = {
                keyword.arg: self._evaluate(keyword.value)
                for keyword in node.keywords
                if keyword.arg is not None
            }
            # A callable argument could be called later, as pydantic calls a
            # `default_factory`, so it has to be a value constructor too.
            for argument in [*args, *kwargs.values()]:
                if callable(argument) and not self._is_value_constructor(argument):
                    raise UnknownExpression(f"callable argument {argument!r}")
            if function in self.rebuilt_classes and issubclass(
                function, pydantic.BaseModel
            ):
                # Validating would build the values of installed field types,
                # like a `BaseSettings` that reads the environment, so the
                # instance is built from the given values as they are.
                if args:
                    raise UnknownExpression("positional arguments to a model")
                return function.model_construct(**kwargs)
            return _bounded(self._apply(function, *args, **kwargs))

        raise UnknownExpression(f"unsupported expression {type(node).__name__}")

    def _reference(self, value: Any) -> Any:
        if not self._is_reference(value):
            raise UnknownExpression(f"reference to the value {type(value).__name__}")
        return value

    def _is_reference(self, value: Any) -> bool:
        """Whether a value reached through an import, attribute, or builtin may
        be used.  Data like `os.environ` may not, because the file could place
        it in a default value that is uploaded with the schema."""
        return (
            isinstance(
                value,
                (type, types.ModuleType, types.GenericAlias, types.UnionType),
            )
            # Enum members and time zones like `datetime.timezone.utc` are
            # constants of the installed code, not configuration.
            or isinstance(value, (enum.Enum, datetime.tzinfo))
            or type(value).__module__ in ("typing", "typing_extensions")
            or self._is_value_constructor(value)
        )

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


def _contains_forward_reference(annotation: Any) -> bool:
    """Whether an annotation holds a string that pydantic would resolve as a
    forward reference.  Pydantic resolves those with `eval`, so a string the
    file built, like `X = "open(...)"` in `def f(x: X)`, would run as code."""
    if isinstance(annotation, (str, typing.ForwardRef)):
        return True
    origin = typing.get_origin(annotation)
    if origin is Literal:
        return False
    arguments = typing.get_args(annotation)
    if origin is Annotated:
        # Pydantic does not resolve strings in `Annotated` metadata.
        arguments = arguments[:1]
    return any(_contains_forward_reference(argument) for argument in arguments)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, complex)) and not isinstance(value, bool)


def _bounded(value: Any) -> Any:
    if isinstance(value, int) and value.bit_length() > MAX_INTEGER_BITS:
        raise UnknownExpression("integer too large")
    if (
        isinstance(value, decimal.Decimal)
        and value.is_finite()
        and abs(value.adjusted()) > MAX_DECIMAL_EXPONENT
    ):
        raise UnknownExpression("decimal too large")
    return value


def signature_from_source(source_code: str, func_name: str) -> inspect.Signature:
    """Read the signature of the function named `func_name` in `source_code`."""
    try:
        module = ast.parse(source_code)
    except (SyntaxError, ValueError, RecursionError, MemoryError) as e:
        raise UnparsableSource(str(e)) from e
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
