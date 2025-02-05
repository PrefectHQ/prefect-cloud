from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import pytest
import toml

from prefect_cloud.auth import (
    Account,
    Workspace,
    cloud_profile_name,
    get_api_key,
    get_api_key_or_login,
    get_cloud_profile,
    load_profiles,
    logout,
    remove_cloud_profile,
    set_cloud_profile,
)


@pytest.fixture
def sample_api_key() -> str:
    """Returns a sample API key for testing."""
    return "pnu_0123456789abcdef0123456789abcdef"


@pytest.fixture
def sample_account() -> Account:
    """Returns a sample Account object for testing."""
    return Account(
        account_id=UUID("11111111-1234-5678-1234-567812345678"),
        account_name="Test Account",
        account_handle="test-account",
    )


@pytest.fixture
def sample_workspace(sample_account: Account) -> Workspace:
    """Returns a sample Workspace object for testing."""
    return Workspace(
        account_id=sample_account.account_id,
        account_name=sample_account.account_name,
        account_handle=sample_account.account_handle,
        workspace_id=UUID("22222222-1234-5678-1234-567812345678"),
        workspace_name="Test Workspace",
        workspace_handle="test-workspace",
    )


@pytest.mark.parametrize(
    "api_url,expected_profile",
    [
        ("https://api.stg.prefect.dev/api", "prefect-cloud-stg"),
        ("https://api.prefect.dev/api", "prefect-cloud-dev"),
        ("http://localhost:4200/api", "prefect-cloud-lcl"),
        ("https://api.prefect.cloud/api", "prefect-cloud"),
    ],
)
def test_cloud_profile_name(
    monkeypatch: pytest.MonkeyPatch, api_url: str, expected_profile: str
):
    """cloud_profile_name() should return the correct profile name based on the API URL."""
    monkeypatch.setattr("prefect_cloud.auth.CLOUD_API_URL", api_url)
    assert cloud_profile_name() == expected_profile


def test_load_profiles_creates_empty_profiles_when_no_file(mock_profiles_path: Path):
    """Verifies that load_profiles() returns empty profiles when no file exists."""
    profiles = load_profiles()
    assert profiles == {"profiles": {}}


def test_get_api_key_returns_none_when_no_profile(mock_profiles_path: Path):
    """Verifies that get_api_key() returns None when no profile exists."""
    assert get_api_key() is None


def test_get_api_key_returns_key_from_profile(
    mock_profiles_path: Path, sample_api_key: str, sample_workspace: Workspace
):
    """Verifies that get_api_key() returns the correct API key from an existing profile."""
    set_cloud_profile(sample_api_key, sample_workspace)
    assert get_api_key() == sample_api_key


def test_get_cloud_profile_no_profiles_file(mock_profiles_path: Path):
    """get_cloud_profile() should return None when profiles file doesn't exist."""
    assert mock_profiles_path.exists()
    mock_profiles_path.unlink()
    assert get_cloud_profile() is None


def test_get_cloud_profile_no_profiles_in_file(mock_profiles_path: Path):
    """get_cloud_profile() should return None when profiles file doesn't exist."""
    assert mock_profiles_path.exists()
    assert get_cloud_profile() is None


def test_set_cloud_profile_creates_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sample_api_key: str,
    sample_workspace: Workspace,
):
    """set_cloud_profile() should create the .prefect directory if it doesn't exist."""
    prefect_home = tmp_path / ".prefect"
    monkeypatch.setattr("prefect_cloud.auth.PREFECT_HOME", prefect_home)

    set_cloud_profile(sample_api_key, sample_workspace)

    assert prefect_home.exists()
    assert (prefect_home / "profiles.toml").exists()


def test_set_cloud_profile_creates_profile_when_none_exists(
    mock_profiles_path: Path,
    sample_api_key: str,
    sample_workspace: Workspace,
):
    """set_cloud_profile() should create a new profile when none exists."""
    set_cloud_profile(sample_api_key, sample_workspace)

    profiles = toml.load(mock_profiles_path)
    assert profiles["active"] == "prefect-cloud"
    assert "prefect-cloud" in profiles["profiles"]
    assert profiles["profiles"]["prefect-cloud"] == {
        "PREFECT_API_KEY": sample_api_key,
        "PREFECT_API_URL": sample_workspace.api_url,
    }


def test_set_cloud_profile_creates_profile_when_empty(
    mock_profiles_path: Path,
    sample_api_key: str,
    sample_workspace: Workspace,
):
    """set_cloud_profile() should create a new profile when none exists."""
    mock_profiles_path.write_text(toml.dumps({}))

    set_cloud_profile(sample_api_key, sample_workspace)

    profiles = toml.load(mock_profiles_path)
    assert profiles["active"] == "prefect-cloud"
    assert "prefect-cloud" in profiles["profiles"]
    assert profiles["profiles"]["prefect-cloud"] == {
        "PREFECT_API_KEY": sample_api_key,
        "PREFECT_API_URL": sample_workspace.api_url,
    }


def test_set_cloud_profile_updates_existing_profile(
    mock_profiles_path: Path,
    sample_api_key: str,
    sample_workspace: Workspace,
):
    """set_cloud_profile() should update an existing profile."""
    # Create initial profile
    profiles = {
        "active": "prefect-cloud",
        "profiles": {
            "prefect-cloud": {
                "PREFECT_API_KEY": "old_key",
                "PREFECT_API_URL": "old_url",
                "SOME_OTHER_SETTING": "value",
            }
        },
    }
    mock_profiles_path.write_text(toml.dumps(profiles))

    set_cloud_profile(sample_api_key, sample_workspace)

    updated_profiles = toml.load(mock_profiles_path)
    assert updated_profiles["profiles"]["prefect-cloud"] == {
        "PREFECT_API_KEY": sample_api_key,
        "PREFECT_API_URL": sample_workspace.api_url,
        "SOME_OTHER_SETTING": "value",
    }


def test_set_cloud_profile_preserves_other_profiles(
    mock_profiles_path: Path,
    sample_api_key: str,
    sample_workspace: Workspace,
):
    """set_cloud_profile() should preserve other profiles when updating."""
    # Create initial profiles
    profiles = {
        "active": "other-profile",
        "profiles": {
            "other-profile": {
                "SOME_SETTING": "value",
            }
        },
    }
    mock_profiles_path.write_text(toml.dumps(profiles))

    set_cloud_profile(sample_api_key, sample_workspace)

    updated_profiles = toml.load(mock_profiles_path)
    assert "other-profile" in updated_profiles["profiles"]
    assert updated_profiles["profiles"]["other-profile"] == {"SOME_SETTING": "value"}
    assert updated_profiles["active"] == "prefect-cloud"


def test_get_api_key_or_login_with_no_key_and_failed_login(mock_profiles_path: Path):
    """get_api_key_or_login() should raise ValueError when no key is available and login fails."""
    with patch("prefect_cloud.auth.login_interactively") as mock_login:
        mock_login.return_value = None
        with pytest.raises(ValueError, match="No API key found"):
            get_api_key_or_login()


def test_get_api_key_or_login_with_no_key_but_successful_login(
    mock_profiles_path: Path,
):
    """get_api_key_or_login() should return key from interactive login when no key is available but login succeeds."""
    test_api_key = "test_key_123"
    with patch("prefect_cloud.auth.login_interactively") as mock_login:
        mock_login.return_value = test_api_key
        result = get_api_key_or_login()
        assert result == test_api_key


def test_get_api_key_or_login_with_existing_key(
    mock_profiles_path: Path, sample_api_key: str
):
    """get_api_key_or_login() should return existing key when one is available."""
    mock_profiles_path.write_text(
        toml.dumps({"profiles": {"prefect-cloud": {"PREFECT_API_KEY": sample_api_key}}})
    )

    with patch("prefect_cloud.auth.login_interactively") as mock_login:
        result = get_api_key_or_login()
        assert result == sample_api_key
        mock_login.assert_not_called()


def test_get_cloud_profile_with_missing_profile(mock_profiles_path: Path):
    """get_cloud_profile() should return None when profile doesn't exist in profiles file."""
    mock_profiles_path.write_text(toml.dumps({"profiles": {}}))
    assert get_cloud_profile() is None


def test_set_cloud_profile_creates_missing_sections(
    mock_profiles_path: Path, sample_api_key: str, sample_workspace: Workspace
):
    """set_cloud_profile() should create missing sections in profiles file."""
    mock_profiles_path.write_text(toml.dumps({}))

    set_cloud_profile(sample_api_key, sample_workspace)

    profiles = toml.load(mock_profiles_path)
    assert "active" in profiles
    assert "profiles" in profiles
    assert cloud_profile_name() in profiles["profiles"]


def test_remove_cloud_profile_with_empty_profiles(mock_profiles_path: Path):
    """remove_cloud_profile() should delete profiles file when no profiles remain."""
    mock_profiles_path.write_text(toml.dumps({"profiles": {"prefect-cloud": {}}}))

    remove_cloud_profile()

    assert not mock_profiles_path.exists()


def test_remove_cloud_profile_with_multiple_profiles(
    mock_profiles_path: Path, sample_api_key: str, sample_workspace: Workspace
):
    """remove_cloud_profile() should handle multiple profiles correctly."""
    profiles = {
        "active": "prefect-cloud",
        "profiles": {
            "prefect-cloud": {
                "PREFECT_API_KEY": sample_api_key,
                "PREFECT_API_URL": sample_workspace.api_url,
            },
            "other-profile": {
                "PREFECT_API_KEY": "other-key",
                "PREFECT_API_URL": "other-url",
            },
        },
    }
    mock_profiles_path.write_text(toml.dumps(profiles))

    logout()

    updated_profiles = toml.load(mock_profiles_path)
    assert "prefect-cloud" not in updated_profiles["profiles"]
    assert updated_profiles["active"] == "other-profile"


def test_remove_cloud_profile_with_single_profile(
    mock_profiles_path: Path, sample_api_key: str, sample_workspace: Workspace
):
    """remove_cloud_profile() should delete profiles file when removing last profile."""
    profiles = {
        "active": "prefect-cloud",
        "profiles": {
            "prefect-cloud": {
                "PREFECT_API_KEY": sample_api_key,
                "PREFECT_API_URL": sample_workspace.api_url,
            }
        },
    }
    mock_profiles_path.write_text(toml.dumps(profiles))

    logout()

    assert not mock_profiles_path.exists()


def test_remove_cloud_profile_with_no_profiles_file(mock_profiles_path: Path):
    """remove_cloud_profile() should handle missing profiles file gracefully."""
    mock_profiles_path.unlink()

    logout()

    assert not mock_profiles_path.exists()


def test_remove_cloud_profile_with_missing_profile(mock_profiles_path: Path):
    """remove_cloud_profile() should handle missing cloud profile gracefully."""
    profiles = {
        "active": "other-profile",
        "profiles": {
            "other-profile": {
                "PREFECT_API_KEY": "other-key",
                "PREFECT_API_URL": "other-url",
            }
        },
    }
    mock_profiles_path.write_text(toml.dumps(profiles))

    logout()

    # Profile file should be unchanged
    updated_profiles = toml.load(mock_profiles_path)
    assert updated_profiles == profiles
