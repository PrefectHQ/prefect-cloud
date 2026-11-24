import datetime
import inspect
from typing import Any, Optional, Union

import pytest

from prefect_cloud.utilities.callables import get_parameter_schema_from_content
from prefect_cloud.utilities.source_signature import signature_from_source


@pytest.mark.parametrize(
    "source_code, expected_annotation, expected_default",
    [
        ("def f(x: int = 1): pass", int, 1),
        ("def f(x: int | None = None): pass", Optional[int], None),
        ("def f(x=-1.5): pass", inspect.Parameter.empty, -1.5),
        (
            "def f(x={'a': [1, (2, 3)]}): pass",
            inspect.Parameter.empty,
            {"a": [1, (2, 3)]},
        ),
        (
            "from typing import Union\nNumber = Union[int, float]\ndef f(x: Number): pass",
            Union[int, float],
            inspect.Parameter.empty,
        ),
        (
            "import datetime\ndef f(x=datetime.timedelta(minutes=5)): pass",
            inspect.Parameter.empty,
            datetime.timedelta(minutes=5),
        ),
        (
            "from datetime import date as d\ndef f(x: d = d(2025, 1, 1)): pass",
            datetime.date,
            datetime.date(2025, 1, 1),
        ),
        ("TIMEOUT = 30\ndef f(x=TIMEOUT): pass", inspect.Parameter.empty, 30),
        ("def f(x: 'Missing' = compute()): pass", inspect.Parameter.empty, None),
        ("def f(x=open('/etc/passwd')): pass", inspect.Parameter.empty, None),
        ("def f(x=[i for i in range(3)]): pass", inspect.Parameter.empty, None),
        ("from typing import Any\ndef f(x: Any): pass", Any, inspect.Parameter.empty),
        ("def f(x: Any): pass", inspect.Parameter.empty, inspect.Parameter.empty),
    ],
)
def test_reads_annotations_and_defaults(
    source_code: str, expected_annotation: Any, expected_default: Any
):
    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.annotation == expected_annotation
    assert parameter.default == expected_default


def test_rebuilds_enums_with_their_mixin():
    source_code = (
        "from enum import Enum\n"
        "class Color(str, Enum):\n"
        "    RED = 'red'\n"
        "    BLUE = 'blue'\n"
        "def f(x: 'Color' = Color.BLUE): pass\n"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert [member.value for member in parameter.annotation] == ["red", "blue"]
    assert isinstance(parameter.default, str)
    assert parameter.default == "blue"


def test_rebuilds_enums_keeping_members_whose_value_is_none():
    source_code = (
        "from enum import Enum\n"
        "class Status(Enum):\n"
        "    UNSET = None\n"
        "    ACTIVE = 'active'\n"
        "def f(x: Status = Status.UNSET): pass\n"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert [member.value for member in parameter.annotation] == [None, "active"]
    assert parameter.default is parameter.annotation.UNSET


def test_rebuilds_pydantic_models_with_field_defaults():
    source_code = (
        "from pydantic import BaseModel, Field\n"
        "class Settings(BaseModel):\n"
        "    retries: int = Field(default=3, description='How many times')\n"
        "    name: str\n"
        "def f(x: Settings = Settings(name='a')): pass\n"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.annotation.model_json_schema()["properties"]["retries"] == {
        "default": 3,
        "description": "How many times",
        "title": "Retries",
        "type": "integer",
    }
    assert parameter.default.model_dump() == {"retries": 3, "name": "a"}


def test_reads_every_parameter_kind():
    source_code = "async def f(a, /, b, *args, c, d=4, **kwargs) -> int: pass"

    signature = signature_from_source(source_code, "f")

    assert [(p.name, p.kind) for p in signature.parameters.values()] == [
        ("a", inspect.Parameter.POSITIONAL_ONLY),
        ("b", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("args", inspect.Parameter.VAR_POSITIONAL),
        ("c", inspect.Parameter.KEYWORD_ONLY),
        ("d", inspect.Parameter.KEYWORD_ONLY),
        ("kwargs", inspect.Parameter.VAR_KEYWORD),
    ]
    assert signature.return_annotation is int


def test_rebuilds_enums_with_auto_values():
    source_code = (
        "import enum\n"
        "class Level(enum.IntEnum):\n"
        "    LOW = enum.auto()\n"
        "    HIGH = enum.auto()\n"
        "def f(x: Level = Level.HIGH): pass\n"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert [member.value for member in parameter.annotation] == [1, 2]
    assert parameter.default == 2


@pytest.mark.parametrize(
    "source_code, expected_default",
    [
        ("def f(x=60 * 60): pass", 3600),
        ("HOUR = 60 * 60\ndef f(x=HOUR * 24 - 1): pass", 86399),
        ("def f(x=1 / 4): pass", 0.25),
        ("def f(x=7 // 2 + 7 % 2): pass", 4),
        ("def f(x=1 / 0): pass", None),
        ("def f(x='a' + 'b'): pass", None),
    ],
)
def test_evaluates_arithmetic_on_numbers(source_code: str, expected_default: Any):
    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.default == expected_default


def test_rebuilds_pydantic_models_with_default_factories():
    source_code = (
        "from pydantic import BaseModel, Field\n"
        "class Tags(BaseModel):\n"
        "    names: list[str] = Field(default_factory=list)\n"
        "def f(x: Tags = Tags()): pass\n"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.default.model_dump() == {"names": []}


@pytest.mark.parametrize(
    "source_code, expected_default",
    [
        (
            "from enum import Enum\nclass C(Enum):\n    A = 'a'\ndef f(x=C.A.value): pass",
            "a",
        ),
        ("D = {'a': [1, 2]}\ndef f(x=D['a'][1]): pass", 2),
        (
            "from pydantic import BaseModel\n"
            "class M(BaseModel):\n"
            "    a: int\n"
            "def f(x=M()): pass",
            None,
        ),
    ],
)
def test_reads_values_from_the_file(source_code: str, expected_default: Any):
    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.default == expected_default


def test_looks_up_enum_members_by_name():
    source_code = (
        "from enum import Enum\nclass C(Enum):\n    A = 'a'\ndef f(x=C['A']): pass"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.default.value == "a"


@pytest.mark.parametrize(
    "annotation, expected_schema",
    [
        ("Literal['a', 'b']", {"enum": ["a", "b"], "type": "string"}),
        (
            "Annotated[str, Field(pattern='^a')]",
            {"pattern": "^a", "type": "string"},
        ),
    ],
)
def test_keeps_strings_in_literals_and_annotated_metadata(
    annotation: str, expected_schema: dict[str, Any]
):
    source_code = (
        "from typing import Annotated, Literal\n"
        "from pydantic import Field\n"
        f"def f(x: {annotation}): pass\n"
    )

    schema = get_parameter_schema_from_content(source_code, "f")

    assert expected_schema.items() <= schema.properties["x"].items()


def test_rebuilds_enums_that_extend_a_member_less_enum():
    source_code = (
        "from enum import Enum\n"
        "class BaseEnum(str, Enum):\n"
        "    @classmethod\n"
        "    def values(cls):\n"
        "        return [member.value for member in cls]\n"
        "class Color(BaseEnum):\n"
        "    RED = 'red'\n"
        "def f(x: Color = Color.RED): pass\n"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert [member.value for member in parameter.annotation] == ["red"]
    assert parameter.default == "red"


def test_rebuilt_models_accept_validation_aliases():
    source_code = (
        "from pydantic import BaseModel, Field\n"
        "class M(BaseModel):\n"
        "    req: int = Field(validation_alias='R')\n"
        "def f(x=M(R=1)): pass\n"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.default is not None


@pytest.mark.parametrize(
    "default, expected_default",
    [
        ("M(a=5)", {"a": 5}),
        ("M(a='5')", {"a": 5}),
        ("M(a='bad')", None),
        ("Outer()", {"inner": {"a": 1}}),
    ],
)
def test_validates_defaults_of_models_with_standard_field_types(
    default: str, expected_default: Any
):
    source_code = (
        "from pydantic import BaseModel, Field\n"
        "class M(BaseModel):\n"
        "    a: int\n"
        "class Inner(BaseModel):\n"
        "    a: int = 1\n"
        "class Outer(BaseModel):\n"
        "    inner: Inner = Field(default_factory=Inner)\n"
        f"def f(x={default}): pass\n"
    )

    schema = get_parameter_schema_from_content(source_code, "f")

    assert schema.properties["x"].get("default") == expected_default
