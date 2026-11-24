"""
Decides which models rebuilt from a flow file pydantic can validate without
running installed code.

Validating a field runs the validators of its type.  For types like
`ImportString`, `FilePath`, or a `BaseSettings` model, those validators import
modules, check files, or read the environment, so a flow file must not choose
the values they validate.  Builtin scalars and containers, the standard value
types, and enums and models rebuilt from the file itself only check and convert
the values they are given.
"""

import datetime
import decimal
import pathlib
import types
import typing
import uuid
from collections.abc import Collection
from typing import Annotated, Any, Literal, Union

import pydantic
import typing_extensions
from pydantic.fields import FieldInfo

SAFE_TYPES: frozenset[Any] = frozenset(
    {
        Any,
        type(None),
        bool,
        int,
        float,
        complex,
        str,
        bytes,
        list,
        dict,
        set,
        frozenset,
        tuple,
        datetime.date,
        datetime.datetime,
        datetime.time,
        datetime.timedelta,
        decimal.Decimal,
        pathlib.Path,
        pathlib.PurePath,
        pathlib.PurePosixPath,
        pathlib.PureWindowsPath,
        uuid.UUID,
    }
)

# Generic origins whose arguments are themselves types to check.
SAFE_ORIGINS: frozenset[Any] = frozenset(
    {list, dict, set, frozenset, tuple, Union, types.UnionType}
)

LITERALS: tuple[Any, ...] = (Literal, typing_extensions.Literal)
ANNOTATED: tuple[Any, ...] = (Annotated, typing_extensions.Annotated)

# Bounds the walk, since a file can build an annotation that repeats itself.
MAX_WALK = 1000


def _is_constraint(metadata: Any) -> bool:
    # `annotated_types` constraints like `Gt` and `MaxLen` only compare values.
    # Other metadata, like pydantic's `PathType`, can check the file system.
    # A `Field(gt=0)` in `Annotated` carries its constraints as metadata too,
    # but validating a missing value would call its `default_factory`.
    if isinstance(metadata, FieldInfo):
        return metadata.default_factory is None and all(
            _is_constraint(inner) for inner in metadata.metadata
        )
    return type(metadata).__module__ == "annotated_types"


def validates_safely(annotation: Any, trusted: Collection[type]) -> bool:
    """Whether validating values of `annotation` runs only pydantic's own
    checks, given the rebuilt classes in `trusted`."""
    pending = [annotation]
    for _ in range(MAX_WALK):
        if not pending:
            return True
        current = pending.pop()
        if current is Ellipsis or current in SAFE_TYPES or current in trusted:
            continue
        origin = typing.get_origin(current)
        arguments = typing.get_args(current)
        if origin in LITERALS:
            continue
        if origin in ANNOTATED:
            if not all(_is_constraint(metadata) for metadata in arguments[1:]):
                return False
            pending.append(arguments[0])
        elif origin in SAFE_ORIGINS:
            pending.extend(arguments)
        else:
            return False
    return False


def model_validates_safely(
    model: type[pydantic.BaseModel], trusted: Collection[type]
) -> bool:
    """Whether validating `model` runs only pydantic's own checks."""
    return all(
        validates_safely(field.annotation, trusted)
        and all(_is_constraint(metadata) for metadata in field.metadata)
        for field in model.model_fields.values()
    )
