from enum import Enum


class PythonVersion(str, Enum):
    PY_312 = "3.12"
    PY_311 = "3.11"
    PY_310 = "3.10"
    PY_39 = "3.9"

    def to_prefect_image(self) -> str:
        return f"prefecthq/prefect-client:3-python{self.value}"
