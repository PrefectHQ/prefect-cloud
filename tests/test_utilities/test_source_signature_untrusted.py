"""
Flow files come from repositories the deploying user may not control, so these
tests write source that tries to run code, read the environment, or exhaust
the reader, and check that none of it gets through.
"""

import ast
import inspect
from pathlib import Path

import pydantic
import pytest

from prefect_cloud.utilities.callables import get_parameter_schema_from_content
from prefect_cloud.utilities.source_signature import (
    SourceNamespace,
    UnparsableSource,
    signature_from_source,
)


@pytest.fixture
def marker(tmp_path: Path) -> Path:
    return tmp_path / "marker"


@pytest.mark.parametrize(
    "source_code",
    [
        (
            "from enum import Enum\n"
            "import pathlib\n"
            "class C(Enum):\n"
            "    _missing_ = pathlib.Path.touch\n"
            "    A = 1\n"
            "def f(x=C(pathlib.Path(MARKER))): pass\n"
        ),
        (
            "from enum import Enum\n"
            "import subprocess\n"
            "class C(subprocess.Popen, Enum):\n"
            "    A = ['touch', MARKER]\n"
            "def f(x: C): pass\n"
        ),
        (
            "from pydantic import BaseModel, Field\n"
            "import pathlib\n"
            "class C(BaseModel):\n"
            "    a: int = Field(default_factory=pathlib.Path(MARKER).touch)\n"
            "def f(x=C()): pass\n"
        ),
        (
            "from pydantic import BaseModel, Field\n"
            "import pathlib\n"
            "class C(BaseModel):\n"
            "    a: int = Field(default_factory=pathlib.Path)\n"
            "touch = pathlib.Path(MARKER).touch\n"
            "def f(x=Field(default_factory=touch).default_factory()): pass\n"
        ),
    ],
)
def test_does_not_run_code_from_the_source(source_code: str, marker: Path):
    source_code = f"MARKER = {str(marker)!r}\n" + source_code

    signature_from_source(source_code, "f")

    assert not marker.exists()


@pytest.mark.parametrize(
    "annotation",
    [
        "X",
        "str(PAYLOAD)",
        "list[X]",
        "Optional[X]",
        "dict[str, X]",
        "Annotated[X, 1]",
        "RootModel[X]",
        "RootModel[list[X]]",
        "RootModel[\"__import__('pathlib').Path(MARKER).touch() or int\"]",
    ],
)
def test_does_not_resolve_strings_built_by_the_source(annotation: str, marker: Path):
    source_code = (
        "from typing import Annotated, Optional\n"
        "from pydantic import RootModel\n"
        f"MARKER = {str(marker)!r}\n"
        f"PAYLOAD = \"open({str(marker)!r}, 'w').write('x')\"\n"
        "X = PAYLOAD\n"
        f"def f(x: {annotation} = 1): pass\n"
    )

    get_parameter_schema_from_content(source_code, "f")

    assert not marker.exists()


@pytest.mark.parametrize(
    "source_code",
    [
        "import os\ndef f(x=os.environ['HOME']): pass",
        "import os\ndef f(x=os.environ): pass",
        "from os import environ\ndef f(x=environ): pass",
        "import sys\ndef f(x=sys.argv): pass",
        "import os\ndef f(x=os.sep): pass",
        (
            "from pydantic_settings import BaseSettings\n"
            "class S(BaseSettings):\n"
            "    home: str\n"
            "def f(x=S()): pass\n"
        ),
        (
            "from pydantic import BaseModel\n"
            "from pydantic_settings import BaseSettings\n"
            "class S(BaseSettings):\n"
            "    home: str\n"
            "class M(BaseModel):\n"
            "    s: S\n"
            "def f(x=M(s={})): pass\n"
        ),
        (
            "import pydantic\n"
            "class Inner(pydantic.BaseModel):\n"
            "    env: pydantic.ImportString[dict[str, str]] = pydantic.Field(\n"
            "        default='os.environ', validate_default=True\n"
            "    )\n"
            "class Outer(pydantic.BaseModel):\n"
            "    inner: Inner = pydantic.Field(default_factory=Inner)\n"
            "def f(x: Outer = Outer()): pass\n"
        ),
        (
            "from pydantic import BaseModel, Field\n"
            "from prefect_cloud.settings import Settings\n"
            "class Inner(BaseModel):\n"
            "    s: Settings = Field(default={}, validate_default=True)\n"
            "class Outer(BaseModel):\n"
            "    i: Inner = Field(default_factory=Inner)\n"
            "def f(x: Outer = Outer()): pass\n"
        ),
        "import pydantic\ndef f(x: str = str(pydantic)): pass",
        "import pydantic\ndef f(x: str = str([pydantic])): pass",
        (
            "import enum, pydantic\n"
            "class E(str, enum.Enum):\n"
            "    A = pydantic\n"
            "    B = (pydantic,)\n"
            "def f(x: E = E.A): pass\n"
        ),
        "import pydantic, typing\ndef f(x: typing.Literal[str(pydantic)]): pass",
        (
            "import pydantic\n"
            "def f(x: pydantic.ImportString[dict[str, str]] = pydantic.Field(\n"
            "    default='os.environ', validate_default=True\n"
            ")): pass\n"
        ),
    ],
)
def test_schema_does_not_include_installed_values(
    source_code: str, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("HOME", "/home/secret")
    monkeypatch.setenv("PREFECT_CLOUD_DEFAULT_MANAGED_WORK_POOL_NAME", "secret")

    schema = get_parameter_schema_from_content(source_code, "f")

    assert "secret" not in schema.model_dump_json()
    assert "site-packages" not in schema.model_dump_json()


def test_rebuilt_model_errors_are_unknown():
    source_code = (
        "import datetime\n"
        "from pydantic import BaseModel, Field\n"
        "class M(BaseModel):\n"
        "    a: datetime.date = Field(default_factory=datetime.date)\n"
        "def f(x: M = M()): pass\n"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.default is None


@pytest.mark.parametrize(
    "source_code",
    [
        "import decimal\ndef f(x=int(decimal.Decimal('1e999999999'))): pass",
        "def f(x=bytes(1000000000000)): pass",
        "A = 0x" + "f" * 200000 + "\nB = A * A\ndef f(x=B): pass",
        "D0 = 1\n"
        + "".join(f"D{i} = [D{i - 1}, D{i - 1}]\n" for i in range(1, 60))
        + "def f(x=D59): pass",
        "D0 = 1\n"
        + "".join(f"D{i} = [D{i - 1}]\n" for i in range(1, 3000))
        + "def f(x=D2999): pass",
        "X0 = 'aaaaaaaaaa'\n"
        + "".join(
            f"X{i} = str([{', '.join([f'X{i - 1}'] * 900)}])\n" for i in (1, 2, 3)
        )
        + "def f(x=X3): pass",
        "import pathlib\nP0 = pathlib.Path('aaaaaaaaaa')\n"
        + "".join(
            f"P{i} = pathlib.Path({', '.join([f'P{i - 1}'] * 50)})\n"
            for i in (1, 2, 3, 4)
        )
        + "def f(x=P4): pass",
        "T0 = (1, 2)\n"
        + "".join(f"T{i} = (T{i - 1}, T{i - 1})\n" for i in range(1, 40))
        + "D = {T39: 1}\ndef f(x=D): pass",
        "A = 99999999\n"
        + "".join(
            f"A{i} = A{i - 1 if i else ''} * A{i - 1 if i else ''}\n" for i in range(40)
        )
        + "def f(x=A39): pass",
    ],
)
def test_huge_values_are_unknown(source_code: str):
    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.default is None


@pytest.mark.parametrize(
    "source_code",
    [
        "def f(x: '" + " | ".join(["int"] * 5000) + "'): pass",
        "def f(x: '\\0'): pass",
        "A0 = int\n"
        + "".join(f"A{i} = tuple[A{i - 1}, A{i - 1}]\n" for i in range(1, 60))
        + "def f(x: A59): pass",
        "from pydantic import RootModel\nA0 = int\n"
        + "".join(f"A{i} = tuple[A{i - 1}, A{i - 1}]\n" for i in range(1, 60))
        + "def f(x: RootModel[A59]): pass",
        "from typing import Literal\nU0 = Literal[0]\n"
        + "".join(f"U{i} = U{i - 1} | Literal[{i}]\n" for i in range(1, 5000))
        + "def f(x: U4999): pass",
    ],
)
def test_unreadable_annotations_are_unknown(source_code: str):
    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert parameter.annotation is inspect.Parameter.empty


@pytest.mark.parametrize(
    "source_code",
    [
        "def f(x:",
        "def f(x): pass\n\0",
    ],
)
def test_unparsable_source_raises(source_code: str):
    with pytest.raises(UnparsableSource):
        signature_from_source(source_code, "f")


@pytest.mark.parametrize(
    "annotation",
    [
        "Annotated[str, Field(pattern='(?=a)b')]",
        "Annotated[int, Field(gt='a')]",
    ],
)
def test_invalid_field_constraints_leave_the_parameter_untyped(annotation: str):
    source_code = (
        "from typing import Annotated\n"
        "from pydantic import Field\n"
        f"def f(x: {annotation}): pass\n"
    )

    schema = get_parameter_schema_from_content(source_code, "f")

    assert "type" not in schema.properties["x"]


def test_stops_rebuilding_models_after_too_many_fields():
    source_code = (
        "from pydantic import BaseModel\nclass M0(BaseModel):\n"
        + "".join(f"    a{i}: int = 0\n" for i in range(500))
        + "".join(f"class M{i}(M{i - 1}):\n    pass\n" for i in range(1, 500))
        + "def f(x: M499): pass"
    )

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert not issubclass(parameter.annotation, pydantic.BaseModel)


def union_chain(length: int) -> ast.expr:
    """An `int | int | ...` expression built directly, because how deep an
    expression `ast.parse` accepts differs between Python builds."""
    node: ast.expr = ast.Name("int")
    for _ in range(length):
        node = ast.BinOp(node, ast.BitOr(), ast.Name("int"))
    return node


def test_deeply_nested_annotations_are_unknown():
    namespace = SourceNamespace(ast.Module(body=[], type_ignores=[]))

    assert namespace.annotation(union_chain(5000)) is inspect.Parameter.empty
