import datetime
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple, Union

import pydantic.version
import pytest
from pydantic import SecretStr

from prefect_cloud.utilities.callables import (
    parameter_schema,
    parameter_schema_from_entrypoint,
)


class TestFunctionToSchema:
    def test_simple_function_with_no_arguments(self):
        def f():
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "properties": {},
            "title": "Parameters",
            "type": "object",
            "definitions": {},
        }

    def test_function_with_pydantic_base_model_collisions(self):
        # TODO: this test actually fails with pydantic v2 attributes like model_dump
        # and friends.  We need a new test for these.
        def f(
            json,
            copy,
            parse_obj,
            parse_raw,
            parse_file,
            from_orm,
            schema,
            schema_json,
            construct,
            validate,
            foo,
        ):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "json": {"title": "json", "position": 0},
                "copy": {"title": "copy", "position": 1},
                "parse_obj": {"title": "parse_obj", "position": 2},
                "parse_raw": {"title": "parse_raw", "position": 3},
                "parse_file": {"title": "parse_file", "position": 4},
                "from_orm": {"title": "from_orm", "position": 5},
                "schema": {"title": "schema", "position": 6},
                "schema_json": {"title": "schema_json", "position": 7},
                "construct": {"title": "construct", "position": 8},
                "validate": {"title": "validate", "position": 9},
                "foo": {"title": "foo", "position": 10},
            },
            "required": [
                "json",
                "copy",
                "parse_obj",
                "parse_raw",
                "parse_file",
                "from_orm",
                "schema",
                "schema_json",
                "construct",
                "validate",
                "foo",
            ],
            "definitions": {},
        }

    def test_function_with_one_required_argument(self):
        def f(x):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"title": "x", "position": 0}},
            "required": ["x"],
            "definitions": {},
        }

    def test_function_with_one_optional_argument(self):
        def f(x=42):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"default": 42, "position": 0, "title": "x"}},
            "definitions": {},
        }

    def test_function_with_one_optional_annotated_argument(self):
        def f(x: int = 42):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {
                    "default": 42,
                    "position": 0,
                    "title": "x",
                    "type": "integer",
                }
            },
            "definitions": {},
        }

    def test_function_with_two_arguments(self):
        def f(x: int, y: float = 5.0):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {"title": "x", "type": "integer", "position": 0},
                "y": {"title": "y", "default": 5.0, "type": "number", "position": 1},
            },
            "required": ["x"],
            "definitions": {},
        }

    def test_function_with_datetime_arguments(self):
        def f(
            a: datetime.date,
            b: datetime.datetime,
            c: datetime.timedelta,
            x: datetime.date = datetime.date(2025, 1, 1),
            y: datetime.datetime = datetime.datetime(
                2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            z: datetime.timedelta = datetime.timedelta(seconds=5),
        ):
            pass

        schema = parameter_schema(f)
        expected_schema = {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "a": {
                    "format": "date",
                    "position": 0,
                    "title": "a",
                    "type": "string",
                },
                "b": {
                    "format": "date-time",
                    "position": 1,
                    "title": "b",
                    "type": "string",
                },
                "c": {
                    "format": "duration",
                    "position": 2,
                    "title": "c",
                    "type": "string",
                },
                "x": {
                    "default": "2025-01-01",
                    "format": "date",
                    "position": 3,
                    "title": "x",
                    "type": "string",
                },
                "y": {
                    "default": "2025-01-01T00:00:00Z",
                    "format": "date-time",
                    "position": 4,
                    "title": "y",
                    "type": "string",
                },
                "z": {
                    "default": "PT5S",
                    "format": "duration",
                    "position": 5,
                    "title": "z",
                    "type": "string",
                },
            },
            "required": ["a", "b", "c"],
            "definitions": {},
        }
        assert schema.model_dump_for_openapi() == expected_schema

    def test_function_with_enum_argument(self):
        class Color(Enum):
            RED = "RED"
            GREEN = "GREEN"
            BLUE = "BLUE"

        def f(x: Color = "RED"):
            pass

        schema = parameter_schema(f)

        expected_schema = {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {
                    "$ref": "#/definitions/Color",
                    "default": "RED",
                    "position": 0,
                    "title": "x",
                }
            },
            "definitions": {
                "Color": {
                    "enum": ["RED", "GREEN", "BLUE"],
                    "title": "Color",
                    "type": "string",
                }
            },
        }

        assert schema.model_dump_for_openapi() == expected_schema

    def test_function_with_generic_arguments(self):
        def f(
            a: List[str],
            b: Dict[str, Any],
            c: Any,
            d: Tuple[int, float],
            e: Union[str, bytes, int],
        ):
            pass

        schema = parameter_schema(f)

        expected_schema = {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "a": {
                    "items": {"type": "string"},
                    "position": 0,
                    "title": "a",
                    "type": "array",
                },
                "b": {"position": 1, "title": "b", "type": "object"},
                "c": {"position": 2, "title": "c"},
                "d": {
                    "maxItems": 2,
                    "minItems": 2,
                    "position": 3,
                    "prefixItems": [{"type": "integer"}, {"type": "number"}],
                    "title": "d",
                    "type": "array",
                },
                "e": {
                    "anyOf": [
                        {"type": "string"},
                        {"format": "binary", "type": "string"},
                        {"type": "integer"},
                    ],
                    "position": 4,
                    "title": "e",
                },
            },
            "required": ["a", "b", "c", "d", "e"],
            "definitions": {},
        }

        assert schema.model_dump_for_openapi() == expected_schema

    def test_function_with_user_defined_type(self):
        class Foo:
            y: int

        def f(x: Foo):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "definitions": {},
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"title": "x", "position": 0}},
            "required": ["x"],
        }

    def test_function_with_user_defined_pydantic_model(self):
        class Foo(pydantic.BaseModel):
            y: int
            z: str

        def f(x: Foo):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "definitions": {
                "Foo": {
                    "properties": {
                        "y": {"title": "Y", "type": "integer"},
                        "z": {"title": "Z", "type": "string"},
                    },
                    "required": ["y", "z"],
                    "title": "Foo",
                    "type": "object",
                }
            },
            "properties": {
                "x": {
                    "$ref": "#/definitions/Foo",
                    "title": "x",
                    "position": 0,
                }
            },
            "required": ["x"],
            "title": "Parameters",
            "type": "object",
        }

    def test_function_with_pydantic_model_default_across_v1_and_v2(self):
        # this import ensures this test imports the installed version of
        # pydantic (not pydantic.v1) and allows us to test that we
        # generate consistent schemas across v1 and v2
        import pydantic

        class Foo(pydantic.BaseModel):
            bar: str

        def f(foo: Foo = Foo(bar="baz")): ...

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "foo": {
                    "$ref": "#/definitions/Foo",
                    "default": {"bar": "baz"},
                    "position": 0,
                    "title": "foo",
                }
            },
            "definitions": {
                "Foo": {
                    "properties": {"bar": {"title": "Bar", "type": "string"}},
                    "required": ["bar"],
                    "title": "Foo",
                    "type": "object",
                }
            },
        }

    def test_function_with_complex_args_across_v1_and_v2(self):
        # this import ensures this test imports the installed version of
        # pydantic (not pydantic.v1) and allows us to test that we
        # generate consistent schemas across v1 and v2
        import pydantic
        import datetime
        from enum import Enum
        from typing import List

        class Foo(pydantic.BaseModel):
            bar: str

        class Color(Enum):
            RED = "RED"
            GREEN = "GREEN"
            BLUE = "BLUE"

        def f(
            a: int,
            s: List[None],
            m: Foo,
            i: int = 0,
            x: float = 1.0,
            model: Foo = Foo(bar="bar"),
            date_arg: datetime.date = datetime.date(2025, 1, 1),
            datetime_arg: datetime.datetime = datetime.datetime(
                2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            timedelta_arg: datetime.timedelta = datetime.timedelta(seconds=5),
            c: Color = Color.BLUE,
        ): ...

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "a": {"position": 0, "title": "a", "type": "integer"},
                "s": {
                    "items": {"type": "null"},
                    "position": 1,
                    "title": "s",
                    "type": "array",
                },
                "m": {
                    "$ref": "#/definitions/Foo",
                    "position": 2,
                    "title": "m",
                },
                "i": {"default": 0, "position": 3, "title": "i", "type": "integer"},
                "x": {"default": 1.0, "position": 4, "title": "x", "type": "number"},
                "model": {
                    "$ref": "#/definitions/Foo",
                    "default": {"bar": "bar"},
                    "position": 5,
                    "title": "model",
                },
                "date_arg": {
                    "default": "2025-01-01",
                    "format": "date",
                    "position": 6,
                    "title": "date_arg",
                    "type": "string",
                },
                "datetime_arg": {
                    "default": "2025-01-01T00:00:00Z",
                    "format": "date-time",
                    "position": 7,
                    "title": "datetime_arg",
                    "type": "string",
                },
                "timedelta_arg": {
                    "default": "PT5S",
                    "format": "duration",
                    "position": 8,
                    "title": "timedelta_arg",
                    "type": "string",
                },
                "c": {
                    "title": "c",
                    "default": "BLUE",
                    "position": 9,
                    "$ref": "#/definitions/Color",
                },
            },
            "required": ["a", "s", "m"],
            "definitions": {
                "Foo": {
                    "properties": {"bar": {"title": "Bar", "type": "string"}},
                    "required": ["bar"],
                    "title": "Foo",
                    "type": "object",
                },
                "Color": {
                    "enum": ["RED", "GREEN", "BLUE"],
                    "title": "Color",
                    "type": "string",
                },
            },
        }

    def test_function_with_secretstr(self):
        def f(x: SecretStr):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {
                    "title": "x",
                    "position": 0,
                    "format": "password",
                    "type": "string",
                    "writeOnly": True,
                },
            },
            "required": ["x"],
            "definitions": {},
        }

    def test_function_with_v1_secretstr_from_compat_module(self):
        import pydantic.v1 as pydantic

        def f(x: pydantic.SecretStr):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {
                    "title": "x",
                    "position": 0,
                },
            },
            "required": ["x"],
            "definitions": {},
        }


class TestMethodToSchema:
    def test_methods_with_no_arguments(self):
        class Foo:
            def f(self):
                pass

            @classmethod
            def g(cls):
                pass

            @staticmethod
            def h():
                pass

        for method in [Foo().f, Foo.g, Foo.h]:
            schema = parameter_schema(method)
            assert schema.model_dump_for_openapi() == {
                "properties": {},
                "title": "Parameters",
                "type": "object",
                "definitions": {},
            }

    def test_methods_with_enum_arguments(self):
        class Color(Enum):
            RED = "RED"
            GREEN = "GREEN"
            BLUE = "BLUE"

        class Foo:
            def f(self, color: Color = "RED"):
                pass

            @classmethod
            def g(cls, color: Color = "RED"):
                pass

            @staticmethod
            def h(color: Color = "RED"):
                pass

        for method in [Foo().f, Foo.g, Foo.h]:
            schema = parameter_schema(method)

            expected_schema = {
                "title": "Parameters",
                "type": "object",
                "properties": {
                    "color": {
                        "$ref": "#/definitions/Color",
                        "default": "RED",
                        "position": 0,
                        "title": "color",
                    }
                },
                "definitions": {
                    "Color": {
                        "enum": ["RED", "GREEN", "BLUE"],
                        "title": "Color",
                        "type": "string",
                    }
                },
            }

            assert schema.model_dump_for_openapi() == expected_schema

    def test_methods_with_complex_arguments(self):
        class Foo:
            def f(self, x: datetime.datetime, y: int = 42, z: Optional[bool] = None):
                pass

            @classmethod
            def g(cls, x: datetime.datetime, y: int = 42, z: Optional[bool] = None):
                pass

            @staticmethod
            def h(x: datetime.datetime, y: int = 42, z: Optional[bool] = None):
                pass

        for method in [Foo().f, Foo.g, Foo.h]:
            schema = parameter_schema(method)
            expected_schema = {
                "title": "Parameters",
                "type": "object",
                "properties": {
                    "x": {
                        "format": "date-time",
                        "position": 0,
                        "title": "x",
                        "type": "string",
                    },
                    "y": {
                        "default": 42,
                        "position": 1,
                        "title": "y",
                        "type": "integer",
                    },
                    "z": {
                        "default": None,
                        "position": 2,
                        "title": "z",
                        "anyOf": [{"type": "boolean"}, {"type": "null"}],
                    },
                },
                "required": ["x"],
                "definitions": {},
            }
            assert schema.model_dump_for_openapi() == expected_schema

    def test_method_with_kwargs_only(self):
        def f(
            *,
            x: int,
        ):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "properties": {"x": {"title": "x", "position": 0, "type": "integer"}},
            "title": "Parameters",
            "type": "object",
            "definitions": {},
            "required": ["x"],
        }


class TestParseFlowDescriptionToSchema:
    def test_flow_with_args_docstring(self):
        def f(x):
            """Function f.

            Args:
                x: required argument x
            """

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {"title": "x", "description": "required argument x", "position": 0}
            },
            "required": ["x"],
            "definitions": {},
        }

    def test_flow_without_docstring(self):
        def f(x):
            pass

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"title": "x", "position": 0}},
            "required": ["x"],
            "definitions": {},
        }

    def test_flow_without_args_docstring(self):
        def f(x):
            """Function f."""

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"title": "x", "position": 0}},
            "required": ["x"],
            "definitions": {},
        }

    def test_flow_with_complex_args_docstring(self):
        def f(x, y):
            """Function f.

            Second line of docstring.

            Args:
                x: required argument x
                y (str): required typed argument y
                  with second line

            Returns:
                None: nothing
            """

        schema = parameter_schema(f)
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {
                    "title": "x",
                    "description": "required argument x",
                    "position": 0,
                },
                "y": {
                    "title": "y",
                    "description": "required typed argument y\nwith second line",
                    "position": 1,
                },
            },
            "required": ["x", "y"],
            "definitions": {},
        }


class TestEntrypointToSchema:
    def test_function_not_found(self, tmp_path: Path):
        source_code = dedent(
            """
        def f():
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)

        with pytest.raises(ValueError):
            parameter_schema_from_entrypoint(f"{tmp_path}/test.py:g")

    def test_simple_function_with_no_arguments(self, tmp_path: Path):
        source_code = dedent(
            """
        def f():
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)

        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "properties": {},
            "title": "Parameters",
            "type": "object",
            "definitions": {},
        }

    def test_function_with_pydantic_base_model_collisions(self, tmp_path: Path):
        source_code = dedent(
            """
        def f(
            json,
            copy,
            parse_obj,
            parse_raw,
            parse_file,
            from_orm,
            schema,
            schema_json,
            construct,
            validate,
            foo,
        ):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "foo": {"title": "foo", "position": 10},
                "json": {"title": "json", "position": 0},
                "copy": {"title": "copy", "position": 1},
                "parse_obj": {"title": "parse_obj", "position": 2},
                "parse_raw": {"title": "parse_raw", "position": 3},
                "parse_file": {"title": "parse_file", "position": 4},
                "from_orm": {"title": "from_orm", "position": 5},
                "schema": {"title": "schema", "position": 6},
                "schema_json": {"title": "schema_json", "position": 7},
                "construct": {"title": "construct", "position": 8},
                "validate": {"title": "validate", "position": 9},
            },
            "required": [
                "json",
                "copy",
                "parse_obj",
                "parse_raw",
                "parse_file",
                "from_orm",
                "schema",
                "schema_json",
                "construct",
                "validate",
                "foo",
            ],
            "definitions": {},
        }

    def test_function_with_one_required_argument(self, tmp_path: Path):
        source_code = dedent(
            """
        def f(x):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"title": "x", "position": 0}},
            "required": ["x"],
            "definitions": {},
        }

    def test_function_with_one_optional_argument(self, tmp_path: Path):
        source_code = dedent(
            """
        def f(x=42):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"title": "x", "default": 42, "position": 0}},
            "definitions": {},
        }

    def test_function_with_one_optional_annotated_argument(self, tmp_path: Path):
        source_code = dedent(
            """
        def f(x: int = 42):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {"title": "x", "default": 42, "type": "integer", "position": 0}
            },
            "definitions": {},
        }

    def test_function_with_two_arguments(self, tmp_path: Path):
        source_code = dedent(
            """
        def f(x: int, y: float = 5.0):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {"title": "x", "type": "integer", "position": 0},
                "y": {"title": "y", "default": 5.0, "type": "number", "position": 1},
            },
            "required": ["x"],
            "definitions": {},
        }

    def test_function_with_datetime_arguments(self, tmp_path: Path):
        source_code = dedent(
            """
        import datetime                 
        from datetime import tzinfo

        def f(
            a: datetime.date,
            b: datetime.datetime,
            c: datetime.timedelta,
            x: datetime.date = datetime.date(2025, 1, 1),
            y: datetime.datetime = datetime.datetime(
                2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            z: datetime.timedelta = datetime.timedelta(seconds=5),
        ):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        expected_schema = {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "a": {
                    "format": "date",
                    "position": 0,
                    "title": "a",
                    "type": "string",
                },
                "b": {
                    "format": "date-time",
                    "position": 1,
                    "title": "b",
                    "type": "string",
                },
                "c": {
                    "format": "duration",
                    "position": 2,
                    "title": "c",
                    "type": "string",
                },
                "x": {
                    "default": "2025-01-01",
                    "format": "date",
                    "position": 3,
                    "title": "x",
                    "type": "string",
                },
                "y": {
                    "default": "2025-01-01T00:00:00Z",
                    "format": "date-time",
                    "position": 4,
                    "title": "y",
                    "type": "string",
                },
                "z": {
                    "default": "PT5S",
                    "format": "duration",
                    "position": 5,
                    "title": "z",
                    "type": "string",
                },
            },
            "required": ["a", "b", "c"],
            "definitions": {},
        }
        assert schema.model_dump_for_openapi() == expected_schema

    def test_function_with_enum_argument(self, tmp_path: Path):
        class Color(Enum):
            RED = "RED"
            GREEN = "GREEN"
            BLUE = "BLUE"

        source_code = dedent(
            """
        from enum import Enum

        class Color(Enum):
            RED = "RED"
            GREEN = "GREEN"
            BLUE = "BLUE"

        def f(x: Color = Color.RED):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")

        expected_schema = {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {
                    "$ref": "#/definitions/Color",
                    "default": "RED",
                    "position": 0,
                    "title": "x",
                }
            },
            "definitions": {
                "Color": {
                    "enum": ["RED", "GREEN", "BLUE"],
                    "title": "Color",
                    "type": "string",
                }
            },
        }
        assert schema.model_dump_for_openapi() == expected_schema

    def test_function_with_generic_arguments(self, tmp_path: Path):
        source_code = dedent(
            """
        from typing import List, Dict, Any, Tuple, Union

        def f(
            a: List[str],
            b: Dict[str, Any],
            c: Any,
            d: Tuple[int, float],
            e: Union[str, bytes, int],
        ):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")

        expected_schema = {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "a": {
                    "items": {"type": "string"},
                    "position": 0,
                    "title": "a",
                    "type": "array",
                },
                "b": {"position": 1, "title": "b", "type": "object"},
                "c": {"position": 2, "title": "c"},
                "d": {
                    "maxItems": 2,
                    "minItems": 2,
                    "position": 3,
                    "prefixItems": [{"type": "integer"}, {"type": "number"}],
                    "title": "d",
                    "type": "array",
                },
                "e": {
                    "anyOf": [
                        {"type": "string"},
                        {"format": "binary", "type": "string"},
                        {"type": "integer"},
                    ],
                    "position": 4,
                    "title": "e",
                },
            },
            "required": ["a", "b", "c", "d", "e"],
            "definitions": {},
        }

        assert schema.model_dump_for_openapi() == expected_schema

    def test_function_with_user_defined_type(self, tmp_path: Path):
        source_code = dedent(
            """
        class Foo:
            y: int

        def f(x: Foo):
            pass
        """
        )

        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"title": "x", "position": 0}},
            "required": ["x"],
            "definitions": {},
        }

    def test_function_with_user_defined_pydantic_model(self, tmp_path: Path):
        source_code = dedent(
            """
        import pydantic

        class Foo(pydantic.BaseModel):
            y: int
            z: str

        def f(x: Foo):
            pass
        """
        )

        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "definitions": {
                "Foo": {
                    "properties": {
                        "y": {"title": "Y", "type": "integer"},
                        "z": {"title": "Z", "type": "string"},
                    },
                    "required": ["y", "z"],
                    "title": "Foo",
                    "type": "object",
                }
            },
            "properties": {
                "x": {
                    "$ref": "#/definitions/Foo",
                    "title": "x",
                    "position": 0,
                }
            },
            "required": ["x"],
            "title": "Parameters",
            "type": "object",
        }

    def test_function_with_pydantic_model_default_across_v1_and_v2(
        self, tmp_path: Path
    ):
        source_code = dedent(
            """
        import pydantic

        class Foo(pydantic.BaseModel):
            bar: str

        def f(foo: Foo = Foo(bar="baz")):
            pass
        """
        )

        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "foo": {
                    "$ref": "#/definitions/Foo",
                    "default": {"bar": "baz"},
                    "position": 0,
                    "title": "foo",
                }
            },
            "definitions": {
                "Foo": {
                    "properties": {"bar": {"title": "Bar", "type": "string"}},
                    "required": ["bar"],
                    "title": "Foo",
                    "type": "object",
                }
            },
        }

    def test_function_with_complex_args_across_v1_and_v2(self, tmp_path: Path):
        source_code = dedent(
            """
        import pydantic
        import datetime
        from enum import Enum
        from typing import List

        class Foo(pydantic.BaseModel):
            bar: str

        class Color(Enum):
            RED = "RED"
            GREEN = "GREEN"
            BLUE = "BLUE"

        def f(
            a: int,
            s: List[None],
            m: Foo,
            i: int = 0,
            x: float = 1.0,
            model: Foo = Foo(bar="bar"),
            date_arg: datetime.date = datetime.date(2025, 1, 1),
            datetime_arg: datetime.datetime = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            timedelta_arg: datetime.timedelta = datetime.timedelta(seconds=5),
            c: Color = Color.BLUE,
        ):
            pass
        """
        )

        datetime_schema = {
            "title": "datetime_arg",
            "default": "2025-01-01T00:00:00Z",
            "position": 7,
            "type": "string",
            "format": "date-time",
        }

        duration_schema = {
            "title": "timedelta_arg",
            "default": "PT5S",
            "position": 8,
            "type": "string",
            "format": "duration",
        }

        enum_schema = {
            "enum": ["RED", "GREEN", "BLUE"],
            "title": "Color",
            "type": "string",
        }

        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")

        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "a": {"position": 0, "title": "a", "type": "integer"},
                "s": {
                    "items": {"type": "null"},
                    "position": 1,
                    "title": "s",
                    "type": "array",
                },
                "m": {
                    "$ref": "#/definitions/Foo",
                    "position": 2,
                    "title": "m",
                },
                "i": {"default": 0, "position": 3, "title": "i", "type": "integer"},
                "x": {"default": 1.0, "position": 4, "title": "x", "type": "number"},
                "model": {
                    "$ref": "#/definitions/Foo",
                    "default": {"bar": "bar"},
                    "position": 5,
                    "title": "model",
                },
                "date_arg": {
                    "default": "2025-01-01",
                    "format": "date",
                    "position": 6,
                    "title": "date_arg",
                    "type": "string",
                },
                "datetime_arg": datetime_schema,
                "timedelta_arg": duration_schema,
                "c": {
                    "title": "c",
                    "default": "BLUE",
                    "position": 9,
                    "$ref": "#/definitions/Color",
                },
            },
            "required": ["a", "s", "m"],
            "definitions": {
                "Foo": {
                    "properties": {"bar": {"title": "Bar", "type": "string"}},
                    "required": ["bar"],
                    "title": "Foo",
                    "type": "object",
                },
                "Color": enum_schema,
            },
        }

    def test_function_with_secretstr(self, tmp_path: Path):
        source_code = dedent(
            """
        from pydantic import SecretStr

        def f(x: SecretStr):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {
                    "title": "x",
                    "position": 0,
                    "format": "password",
                    "type": "string",
                    "writeOnly": True,
                },
            },
            "required": ["x"],
            "definitions": {},
        }

    def test_function_with_v1_secretstr_from_compat_module(self, tmp_path: Path):
        source_code = dedent(
            """
        import pydantic.v1 as pydantic

        def f(x: pydantic.SecretStr):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {
                    "title": "x",
                    "position": 0,
                },
            },
            "required": ["x"],
            "definitions": {},
        }

    def test_flow_with_args_docstring(self, tmp_path: Path):
        source_code = dedent(
            '''
        def f(x):
            """Function f.

            Args:
                x: required argument x
            """
        '''
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {"title": "x", "description": "required argument x", "position": 0}
            },
            "required": ["x"],
            "definitions": {},
        }

    def test_flow_without_args_docstring(self, tmp_path: Path):
        source_code = dedent(
            '''
        def f(x):
            """Function f."""
        '''
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"title": "x", "position": 0}},
            "required": ["x"],
            "definitions": {},
        }

    def test_flow_with_complex_args_docstring(self, tmp_path: Path):
        source_code = dedent(
            '''
        def f(x, y):
            """Function f.

            Second line of docstring.

            Args:
                x: required argument x
                y (str): required typed argument y
                  with second line

            Returns:
                None: nothing
            """
        '''
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "x": {
                    "title": "x",
                    "description": "required argument x",
                    "position": 0,
                },
                "y": {
                    "title": "y",
                    "description": "required typed argument y\nwith second line",
                    "position": 1,
                },
            },
            "required": ["x", "y"],
            "definitions": {},
        }

    def test_does_not_raise_when_missing_dependencies(self, tmp_path: Path):
        source_code = dedent(
            """
        import bipitty_boopity
                             
        def f(x):
            pass
        """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")

        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {"x": {"title": "x", "position": 0}},
            "required": ["x"],
            "definitions": {},
        }

    def test_handles_dynamically_created_models(self, tmp_path: Path):
        source_code = dedent(
            """
            from pydantic import BaseModel, create_model, Field


            def get_model() -> BaseModel:
                return create_model(
                    "MyModel",
                    param=(
                        int,
                        Field(
                            title="param",
                            default=1,
                        ),
                    ),
                )


            MyModel = get_model()


            def f(
                param: MyModel,
            ) -> None:
                pass        
            """
        )
        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "title": "Parameters",
            "type": "object",
            "properties": {
                "param": {
                    "$ref": "#/definitions/MyModel",
                    "position": 0,
                    "title": "param",
                }
            },
            "required": ["param"],
            "definitions": {
                "MyModel": {
                    "properties": {
                        "param": {
                            "default": 1,
                            "title": "param",
                            "type": "integer",
                        }
                    },
                    "title": "MyModel",
                    "type": "object",
                }
            },
        }

    def test_function_with_kwargs_only(self, tmp_path: Path):
        source_code = dedent(
            """
        def f(
            *,
            x: int = 42,
        ):
            pass
        """
        )

        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "properties": {
                "x": {"title": "x", "position": 0, "type": "integer", "default": 42}
            },
            "title": "Parameters",
            "type": "object",
            "definitions": {},
        }

    def test_function_with_positional_only_args(self, tmp_path: Path):
        source_code = dedent(
            """
        def f(x=1, /, y=2, z=3):
            pass
        """
        )

        tmp_path.joinpath("test.py").write_text(source_code)
        schema = parameter_schema_from_entrypoint(f"{tmp_path}/test.py:f")
        assert schema.model_dump_for_openapi() == {
            "properties": {
                "x": {"title": "x", "position": 0, "default": 1},
                "y": {"title": "y", "position": 1, "default": 2},
                "z": {"title": "z", "position": 2, "default": 3},
            },
            "title": "Parameters",
            "type": "object",
            "definitions": {},
        }
