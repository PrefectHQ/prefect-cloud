"""
Flow files come from repositories the deploying user may not control, so these
tests write source that tries to run code, read the environment, or exhaust
the reader, and check that none of it gets through.
"""

import inspect
from pathlib import Path

import pytest

from prefect_cloud.utilities.callables import get_parameter_schema_from_content
from prefect_cloud.utilities.source_signature import (
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
    ],
)
def test_does_not_resolve_strings_built_by_the_source(annotation: str, marker: Path):
    source_code = (
        "from typing import Annotated, Optional\n"
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
    ],
)
def test_defaults_do_not_read_installed_values(
    source_code: str, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("HOME", "/home/secret")

    parameter = signature_from_source(source_code, "f").parameters["x"]

    assert "secret" not in repr(parameter.default)
    assert "/" not in repr(parameter.default)


@pytest.mark.parametrize(
    "source_code",
    [
        "import decimal\ndef f(x=int(decimal.Decimal('1e999999999'))): pass",
        "def f(x=bytes(1000000000000)): pass",
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
        "def f(x: " + " | ".join(["int"] * 5000) + "): pass",
        "def f(x: '" + " | ".join(["int"] * 5000) + "'): pass",
        "def f(x: '\\0'): pass",
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
