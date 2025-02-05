from pathlib import Path

import pytest
import respx
import toml


@pytest.fixture(autouse=True)
def mock_profiles_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Provides a temporary path for profile storage."""
    profiles_path = tmp_path / "profiles.toml"
    profiles_path.write_text(toml.dumps({"profiles": {}}))
    monkeypatch.setattr("prefect_cloud.auth.PREFECT_HOME", tmp_path)
    return profiles_path


@pytest.fixture(autouse=True)
def cloud_api(mock_profiles_path: Path):
    """
    Automatically mock all HTTP calls and fail on any unexpected requests.

    This helps catch any tests that might try to make real HTTP calls.
    """
    with respx.mock(
        base_url="https://api.prefect.cloud/api",
        assert_all_called=False,
        assert_all_mocked=True,
    ) as mock:
        mock.route(host="localhost").pass_through()
        yield mock
