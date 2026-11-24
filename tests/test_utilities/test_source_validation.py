import datetime
import pathlib
from typing import Annotated, Any, Literal, Optional, Union

import pydantic
import pytest
from pydantic_settings import BaseSettings

from prefect_cloud.utilities.source_validation import (
    model_validates_safely,
    validates_safely,
)


class Trusted(pydantic.BaseModel):
    a: int


class Settings(BaseSettings):
    a: int = 0


@pytest.mark.parametrize(
    "annotation, expected",
    [
        (int, True),
        (Any, True),
        (Optional[list[dict[str, int]]], True),
        (Union[datetime.date, pathlib.Path], True),
        (tuple[int, ...], True),
        (Literal["a", 1], True),
        (Annotated[int, pydantic.Field(gt=0)], True),
        (Trusted, True),
        (list[Trusted], True),
        (pydantic.FilePath, False),
        (pydantic.ImportString, False),
        (pydantic.SecretStr, False),
        (Settings, False),
        (Optional[Settings], False),
        (Annotated[int, pydantic.BeforeValidator(int)], False),
        (Annotated[str, pydantic.Field(default_factory=pathlib.Path.cwd)], False),
    ],
)
def test_validates_safely(annotation: Any, expected: bool):
    assert validates_safely(annotation, trusted=[Trusted]) is expected


@pytest.mark.parametrize(
    "model, expected",
    [
        (pydantic.create_model("Plain", a=(int, 1), b=(Trusted, ...)), True),
        (
            pydantic.create_model(
                "Constrained", a=(int, pydantic.Field(default=1, ge=0))
            ),
            True,
        ),
        (pydantic.create_model("WithFile", p=(pydantic.FilePath, ...)), False),
        (pydantic.create_model("WithSettings", s=(Settings, ...)), False),
    ],
)
def test_model_validates_safely(model: type[pydantic.BaseModel], expected: bool):
    assert model_validates_safely(model, trusted=[Trusted]) is expected
