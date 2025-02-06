"""
Utilities for extensions of and operations on Python collections.
"""

import io
import types
import warnings
from collections import OrderedDict
from collections.abc import (
    Callable,
    Hashable,
)
from dataclasses import fields, is_dataclass, replace
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    Union,
    cast,
    overload,
)
from unittest.mock import Mock

import pydantic
from typing_extensions import TypeAlias, TypeVar

from prefect_cloud.utilities.annotations import BaseAnnotation as BaseAnnotation
from prefect_cloud.utilities.annotations import Quote as Quote
from prefect_cloud.utilities.annotations import quote as quote

if TYPE_CHECKING:
    pass


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


KT = TypeVar("KT")
VT = TypeVar("VT", infer_variance=True)
VT1 = TypeVar("VT1", infer_variance=True)
VT2 = TypeVar("VT2", infer_variance=True)
R = TypeVar("R", infer_variance=True)
NestedDict: TypeAlias = dict[KT, Union[VT, "NestedDict[KT, VT]"]]
HashableT = TypeVar("HashableT", bound=Hashable)


def isiterable(obj: Any) -> bool:
    """
    Return a boolean indicating if an object is iterable.

    Excludes types that are iterable but typically used as singletons:
    - str
    - bytes
    - IO objects
    """
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return not isinstance(obj, (str, bytes, io.IOBase))


class StopVisiting(BaseException):
    """
    A special exception used to stop recursive visits in `visit_collection`.

    When raised, the expression is returned without modification and recursive visits
    in that path will end.
    """


@overload
def visit_collection(
    expr: Any,
    visit_fn: Callable[[Any, dict[str, VT]], Any],
    *,
    return_data: Literal[True] = ...,
    max_depth: int = ...,
    context: dict[str, VT] = ...,
    remove_annotations: bool = ...,
    _seen: Optional[set[int]] = ...,
) -> Any: ...


@overload
def visit_collection(
    expr: Any,
    visit_fn: Callable[[Any], Any],
    *,
    return_data: Literal[True] = ...,
    max_depth: int = ...,
    context: None = None,
    remove_annotations: bool = ...,
    _seen: Optional[set[int]] = ...,
) -> Any: ...


@overload
def visit_collection(
    expr: Any,
    visit_fn: Callable[[Any, dict[str, VT]], Any],
    *,
    return_data: bool = ...,
    max_depth: int = ...,
    context: dict[str, VT] = ...,
    remove_annotations: bool = ...,
    _seen: Optional[set[int]] = ...,
) -> Optional[Any]: ...


@overload
def visit_collection(
    expr: Any,
    visit_fn: Callable[[Any], Any],
    *,
    return_data: bool = ...,
    max_depth: int = ...,
    context: None = None,
    remove_annotations: bool = ...,
    _seen: Optional[set[int]] = ...,
) -> Optional[Any]: ...


@overload
def visit_collection(
    expr: Any,
    visit_fn: Callable[[Any, dict[str, VT]], Any],
    *,
    return_data: Literal[False] = False,
    max_depth: int = ...,
    context: dict[str, VT] = ...,
    remove_annotations: bool = ...,
    _seen: Optional[set[int]] = ...,
) -> None: ...


def visit_collection(
    expr: Any,
    visit_fn: Union[Callable[[Any, dict[str, VT]], Any], Callable[[Any], Any]],
    *,
    return_data: bool = False,
    max_depth: int = -1,
    context: Optional[dict[str, VT]] = None,
    remove_annotations: bool = False,
    _seen: Optional[set[int]] = None,
) -> Optional[Any]:
    """
    Visits and potentially transforms every element of an arbitrary Python collection.

    If an element is a Python collection, it will be visited recursively. If an element
    is not a collection, `visit_fn` will be called with the element. The return value of
    `visit_fn` can be used to alter the element if `return_data` is set to `True`.

    Note:
    - When `return_data` is `True`, a copy of each collection is created only if
      `visit_fn` modifies an element within that collection. This approach minimizes
      performance penalties by avoiding unnecessary copying.
    - When `return_data` is `False`, no copies are created, and only side effects from
      `visit_fn` are applied. This mode is faster and should be used when no transformation
      of the collection is required, because it never has to copy any data.

    Supported types:
    - List (including iterators)
    - Tuple
    - Set
    - Dict (note: keys are also visited recursively)
    - Dataclass
    - Pydantic model
    - Prefect annotations

    Note that visit_collection will not consume generators or async generators, as it would prevent
    the caller from iterating over them.

    Args:
        expr (Any): A Python object or expression.
        visit_fn (Callable[[Any, Optional[dict]], Any] or Callable[[Any], Any]): A function
            that will be applied to every non-collection element of `expr`. The function can
            accept one or two arguments. If two arguments are accepted, the second argument
            will be the context dictionary.
        return_data (bool): If `True`, a copy of `expr` containing data modified by `visit_fn`
            will be returned. This is slower than `return_data=False` (the default).
        max_depth (int): Controls the depth of recursive visitation. If set to zero, no
            recursion will occur. If set to a positive integer `N`, visitation will only
            descend to `N` layers deep. If set to any negative integer, no limit will be
            enforced and recursion will continue until terminal items are reached. By
            default, recursion is unlimited.
        context (Optional[dict]): An optional dictionary. If passed, the context will be sent
            to each call to the `visit_fn`. The context can be mutated by each visitor and
            will be available for later visits to expressions at the given depth. Values
            will not be available "up" a level from a given expression.
            The context will be automatically populated with an 'annotation' key when
            visiting collections within a `BaseAnnotation` type. This requires the caller to
            pass `context={}` and will not be activated by default.
        remove_annotations (bool): If set, annotations will be replaced by their contents. By
            default, annotations are preserved but their contents are visited.
        _seen (Optional[Set[int]]): A set of object ids that have already been visited. This
            prevents infinite recursion when visiting recursive data structures.

    Returns:
        Any: The modified collection if `return_data` is `True`, otherwise `None`.
    """

    if _seen is None:
        _seen = set()

    if context is not None:
        _callback = cast(Callable[[Any, dict[str, VT]], Any], visit_fn)

        def visit_nested(expr: Any) -> Optional[Any]:
            return visit_collection(
                expr,
                _callback,
                return_data=return_data,
                remove_annotations=remove_annotations,
                max_depth=max_depth - 1,
                # Copy the context on nested calls so it does not "propagate up"
                context=context.copy(),
                _seen=_seen,
            )

        def visit_expression(expr: Any) -> Any:
            return _callback(expr, context)
    else:
        _callback = cast(Callable[[Any], Any], visit_fn)

        def visit_nested(expr: Any) -> Optional[Any]:
            # Utility for a recursive call, preserving options and updating the depth.
            return visit_collection(
                expr,
                _callback,
                return_data=return_data,
                remove_annotations=remove_annotations,
                max_depth=max_depth - 1,
                _seen=_seen,
            )

        def visit_expression(expr: Any) -> Any:
            return _callback(expr)

    # --- 1. Visit every expression
    try:
        result = visit_expression(expr)
    except StopVisiting:
        max_depth = 0
        result = expr

    if return_data:
        # Only mutate the root expression if the user indicated we're returning data,
        # otherwise the function could return null and we have no collection to check
        expr = result

    # --- 2. Visit every child of the expression recursively

    # If we have reached the maximum depth or we have already visited this object,
    # return the result if we are returning data, otherwise return None
    if max_depth == 0 or id(expr) in _seen:
        return result if return_data else None
    else:
        _seen.add(id(expr))

    # Then visit every item in the expression if it is a collection

    # presume that the result is the original expression.
    # in each of the following cases, we will update the result if we need to.
    result = expr

    # --- Generators

    if isinstance(expr, (types.GeneratorType, types.AsyncGeneratorType)):
        # Do not attempt to iterate over generators, as it will exhaust them
        pass

    # --- Mocks

    elif isinstance(expr, Mock):
        # Do not attempt to recurse into mock objects
        pass

    # --- Annotations (unmapped, quote, etc.)

    elif isinstance(expr, BaseAnnotation):
        annotated = cast(BaseAnnotation[Any], expr)
        if context is not None:
            context["annotation"] = cast(VT, annotated)
        unwrapped = annotated.unwrap()
        value = visit_nested(unwrapped)

        if return_data:
            # if we are removing annotations, return the value
            if remove_annotations:
                result = value
            # if the value was modified, rewrap it
            elif value is not unwrapped:
                result = annotated.rewrap(value)
            # otherwise return the expr

    # --- Sequences

    elif isinstance(expr, (list, tuple, set)):
        seq = cast(Union[list[Any], tuple[Any], set[Any]], expr)
        items = [visit_nested(o) for o in seq]
        if return_data:
            modified = any(item is not orig for item, orig in zip(items, seq))
            if modified:
                result = type(seq)(items)

    # --- Dictionaries

    elif isinstance(expr, (dict, OrderedDict)):
        mapping = cast(dict[Any, Any], expr)
        items = [(visit_nested(k), visit_nested(v)) for k, v in mapping.items()]
        if return_data:
            modified = any(
                k1 is not k2 or v1 is not v2
                for (k1, v1), (k2, v2) in zip(items, mapping.items())
            )
            if modified:
                result = type(mapping)(items)

    # --- Dataclasses

    elif is_dataclass(expr) and not isinstance(expr, type):
        expr_fields = fields(expr)
        values = [visit_nested(getattr(expr, f.name)) for f in expr_fields]
        if return_data:
            modified = any(
                getattr(expr, f.name) is not v for f, v in zip(expr_fields, values)
            )
            if modified:
                result = replace(
                    expr, **{f.name: v for f, v in zip(expr_fields, values)}
                )

    # --- Pydantic models

    elif isinstance(expr, pydantic.BaseModel):
        # when extra=allow, fields not in model_fields may be in model_fields_set
        model_fields = expr.model_fields_set.union(expr.model_fields.keys())

        # We may encounter a deprecated field here, but this isn't the caller's fault
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=DeprecationWarning)

            updated_data = {
                field: visit_nested(getattr(expr, field)) for field in model_fields
            }

        if return_data:
            modified = any(
                getattr(expr, field) is not updated_data[field]
                for field in model_fields
            )
            if modified:
                # Use construct to avoid validation and handle immutability
                model_instance = expr.model_construct(
                    _fields_set=expr.model_fields_set, **updated_data
                )
                for private_attr in expr.__private_attributes__:
                    setattr(model_instance, private_attr, getattr(expr, private_attr))
                result = model_instance

    if return_data:
        return result
