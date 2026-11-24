import datetime
import inspect
from typing import Any, Optional, Union

import pytest

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
