from prefect_cloud.py_versions import PythonVersion


def test_python_version_values():
    assert PythonVersion.PY_312 == "3.12"
    assert PythonVersion.PY_311 == "3.11"
    assert PythonVersion.PY_310 == "3.10"
    assert PythonVersion.PY_39 == "3.9"


def test_to_prefect_image():
    assert (
        PythonVersion.PY_312.to_prefect_image()
        == "prefecthq/prefect-client:3-python3.12"
    )
    assert (
        PythonVersion.PY_311.to_prefect_image()
        == "prefecthq/prefect-client:3-python3.11"
    )
    assert (
        PythonVersion.PY_310.to_prefect_image()
        == "prefecthq/prefect-client:3-python3.10"
    )
    assert (
        PythonVersion.PY_39.to_prefect_image() == "prefecthq/prefect-client:3-python3.9"
    )
