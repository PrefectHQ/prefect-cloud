from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest
import toml
from httpx import HTTPStatusError
from respx import Router

from prefect_cloud.auth import (
    Account,
    Me,
    Workspace,
    cloud_client,
    get_accounts,
    get_cloud_profile,
    get_cloud_urls_or_login,
    get_cloud_urls_without_login,
    get_workspaces,
    key_is_valid,
    login,
    logout,
    lookup_workspace,
    me,
    prompt_for_workspace,
    set_cloud_profile,
)


@pytest.fixture
def sample_api_key() -> str:
    """Returns a sample API key for testing."""
    return "pnu_0123456789abcdef0123456789abcdef"


@pytest.fixture
def sample_me() -> Me:
    """Returns a sample Me object representing a user."""
    return Me(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        email="test@example.com",
        first_name="Test",
        last_name="User",
        handle="testuser",
    )


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


def test_workspace_full_handle(sample_workspace: Workspace):
    """Workspace full handle should combine account and workspace handles."""
    assert sample_workspace.full_handle == "test-account/test-workspace"


def test_workspace_api_url(sample_workspace: Workspace):
    """Verifies that workspace API URL is correctly constructed."""
    expected = (
        "https://api.prefect.cloud/api/accounts/"
        "11111111-1234-5678-1234-567812345678/workspaces/"
        "22222222-1234-5678-1234-567812345678"
    )
    assert sample_workspace.api_url == expected


async def test_cloud_client_sets_auth_header(sample_api_key: str):
    """Verifies that cloud client sets the correct authorization header and base URL."""
    async with cloud_client(sample_api_key) as client:
        assert client.headers["Authorization"] == f"Bearer {sample_api_key}"
        assert client.base_url == httpx.URL("https://api.prefect.cloud/api/")


async def test_key_is_valid_with_valid_key(cloud_api: Router, sample_api_key: str):
    """API key validation should return True for valid keys."""
    cloud_api.get("https://api.prefect.cloud/api/me/").mock(
        return_value=httpx.Response(200)
    )
    assert await key_is_valid(sample_api_key) is True


async def test_key_is_valid_with_invalid_key(cloud_api: Router, sample_api_key: str):
    """API key validation should return False for invalid keys."""
    cloud_api.get("https://api.prefect.cloud/api/me/").mock(
        return_value=httpx.Response(401)
    )
    assert await key_is_valid(sample_api_key) is False


async def test_me_returns_user_info(
    cloud_api: Router, sample_api_key: str, sample_me: Me
):
    """me() should return the authenticated user's information."""
    cloud_api.get("https://api.prefect.cloud/api/me/").mock(
        return_value=httpx.Response(200, json=sample_me.model_dump(mode="json"))
    )
    result = await me(sample_api_key)
    assert result == sample_me


async def test_me_handles_http_error(cloud_api: Router, sample_api_key: str):
    """me() should handle HTTP errors gracefully."""
    cloud_api.get("/me/").mock(return_value=httpx.Response(500))

    with pytest.raises(HTTPStatusError):
        await me(sample_api_key)


async def test_get_accounts_returns_accounts(
    cloud_api: Router, sample_api_key: str, sample_account: Account
):
    """get_accounts() should return the list of accounts for the authenticated user."""
    cloud_api.get("https://api.prefect.cloud/api/me/accounts").mock(
        return_value=httpx.Response(200, json=[sample_account.model_dump(mode="json")])
    )
    result = await get_accounts(sample_api_key)
    assert result == [sample_account]


async def test_get_accounts_handles_http_error(cloud_api: Router, sample_api_key: str):
    """get_accounts() should handle HTTP errors gracefully."""
    cloud_api.get("/me/accounts").mock(return_value=httpx.Response(500))

    with pytest.raises(HTTPStatusError):
        await get_accounts(sample_api_key)


async def test_get_workspaces_returns_sorted_workspaces(
    cloud_api: Router, sample_api_key: str, sample_workspace: Workspace
):
    """get_workspaces() should return workspaces sorted by handle."""
    workspace2 = Workspace(
        account_id=sample_workspace.account_id,
        account_name=sample_workspace.account_name,
        account_handle="another-account",
        workspace_id=UUID("33333333-1234-5678-1234-567812345678"),
        workspace_name="Another Workspace",
        workspace_handle="another-workspace",
    )

    cloud_api.get("/me/workspaces").mock(
        return_value=httpx.Response(
            200,
            json=[
                sample_workspace.model_dump(mode="json"),
                workspace2.model_dump(mode="json"),
            ],
        )
    )
    result = await get_workspaces(sample_api_key)
    assert result == [workspace2, sample_workspace]


async def test_get_workspaces_handles_http_error(
    cloud_api: Router, sample_api_key: str
):
    """get_workspaces() should handle HTTP errors gracefully."""
    cloud_api.get("/me/workspaces").mock(return_value=httpx.Response(500))

    with pytest.raises(HTTPStatusError):
        await get_workspaces(sample_api_key)


@pytest.mark.parametrize(
    "identifier,expected_found",
    [
        ("22222222-1234-5678-1234-567812345678", True),  # By UUID
        ("test-account/test-workspace", True),  # By full handle
        ("test-workspace", True),  # By workspace handle
        ("nonexistent", False),  # Not found
    ],
)
async def test_lookup_workspace(
    cloud_api: Router,
    sample_api_key: str,
    sample_workspace: Workspace,
    identifier: str,
    expected_found: bool,
):
    """Workspace lookup should find workspaces by UUID, full handle, or workspace handle."""
    cloud_api.get("/me/workspaces").mock(
        return_value=httpx.Response(
            200, json=[sample_workspace.model_dump(mode="json")]
        )
    )

    result = await lookup_workspace(sample_api_key, identifier)

    if expected_found:
        assert result == sample_workspace
    else:
        assert result is None


async def test_prompt_for_workspace_single_workspace(
    cloud_api: Router, sample_api_key: str, sample_workspace: Workspace
):
    """prompt_for_workspace() should return the workspace directly if there's only one."""
    cloud_api.get("/me/workspaces").mock(
        return_value=httpx.Response(
            200, json=[sample_workspace.model_dump(mode="json")]
        )
    )

    result = await prompt_for_workspace(sample_api_key)
    assert result == sample_workspace


async def test_prompt_for_workspace_multiple_workspaces(
    cloud_api: Router, sample_api_key: str, sample_workspace: Workspace
):
    """prompt_for_workspace() should prompt user when multiple workspaces exist."""
    workspace2 = Workspace(
        account_id=sample_workspace.account_id,
        account_name=sample_workspace.account_name,
        account_handle="another-account",
        workspace_id=UUID("33333333-1234-5678-1234-567812345678"),
        workspace_name="Another Workspace",
        workspace_handle="another-workspace",
    )

    cloud_api.get("/me/workspaces").mock(
        return_value=httpx.Response(
            200,
            json=[
                workspace2.model_dump(mode="json"),
                sample_workspace.model_dump(mode="json"),
            ],
        )
    )

    with patch("prefect_cloud.auth.prompt_select_from_list") as mock_prompt:
        mock_prompt.return_value = sample_workspace.full_handle
        result = await prompt_for_workspace(sample_api_key)
        assert result == sample_workspace
        mock_prompt.assert_called_once()


async def test_login_with_no_api_key_and_no_existing_profile(
    cloud_api: Router, mock_profiles_path: Path, sample_workspace: Workspace
):
    """login() should attempt interactive login when no API key is provided."""
    test_api_key = "test_key_123"
    cloud_api.get("/me/workspaces").mock(
        return_value=httpx.Response(
            200, json=[sample_workspace.model_dump(mode="json")]
        )
    )

    with patch("prefect_cloud.auth.login_interactively") as mock_login:
        mock_login.return_value = test_api_key
        await login()
        mock_login.assert_called_once()


async def test_login_with_invalid_api_key(cloud_api: Router):
    """login() should handle invalid API keys gracefully."""
    cloud_api.get("/me/").mock(return_value=httpx.Response(401))

    # cloud_api.get("/me/workspaces").mock(
    #     return_value=httpx.Response(
    #     401, json=[]
    #     )
    # )
    with patch("prefect_cloud.auth.login_interactively") as mock_login:
        mock_login.return_value = None
        await login("invalid_key")
        assert mock_login.called


async def test_login_with_invalid_api_key_but_successful_interactive_login(
    cloud_api: Router, sample_api_key: str, sample_workspace: Workspace
):
    """login() should fall back to interactive login when the provided key is invalid."""
    cloud_api.get("/me/").mock(return_value=httpx.Response(401))
    cloud_api.get("/me/workspaces").mock(
        return_value=httpx.Response(
            200, json=[sample_workspace.model_dump(mode="json")]
        )
    )

    with patch("prefect_cloud.auth.login_interactively") as mock_login:
        mock_login.return_value = sample_api_key
        await login("invalid_key")
        assert mock_login.called


async def test_login_with_workspace_id(
    cloud_api: Router, sample_api_key: str, sample_workspace: Workspace
):
    """login() should accept a workspace ID and set the profile correctly."""
    cloud_api.get("/me/").mock(return_value=httpx.Response(200))
    cloud_api.get("/me/workspaces").mock(
        return_value=httpx.Response(
            200, json=[sample_workspace.model_dump(mode="json")]
        )
    )

    assert get_cloud_profile() is None

    await login(sample_api_key, str(sample_workspace.workspace_id))

    profile = get_cloud_profile()
    assert profile is not None
    assert profile["PREFECT_API_KEY"] == sample_api_key
    assert profile["PREFECT_API_URL"] == sample_workspace.api_url


async def test_login_with_workspace_not_found(cloud_api: Router, sample_api_key: str):
    """login() should handle the case when specified workspace is not found."""
    cloud_api.get("/me/").mock(return_value=httpx.Response(200, json={}))
    cloud_api.get("/me/workspaces").mock(return_value=httpx.Response(200, json=[]))
    result = await login(sample_api_key, "nonexistent")
    assert result is None


async def test_login_with_no_workspaces(cloud_api: Router, sample_api_key: str):
    """login() should handle the case when no workspaces are available."""
    cloud_api.get("/me/").mock(return_value=httpx.Response(200))
    cloud_api.get("/me/workspaces").mock(return_value=httpx.Response(200, json=[]))

    result = await login(sample_api_key)
    assert result is None


async def test_login_with_valid_key_but_workspace_selection_cancelled(
    cloud_api: Router, sample_api_key: str, sample_workspace: Workspace
):
    """login() should handle cancelled workspace selection."""
    cloud_api.get("/me/").mock(return_value=httpx.Response(200))
    cloud_api.get("/me/workspaces").mock(
        return_value=httpx.Response(
            200,
            json=[
                sample_workspace.model_dump(mode="json"),
            ],
        )
    )

    with patch("prefect_cloud.auth.prompt_for_workspace") as mock_prompt:
        mock_prompt.return_value = None
        result = await login(sample_api_key)
        assert result is None


def test_logout_removes_profile(
    mock_profiles_path: Path, sample_api_key: str, sample_workspace: Workspace
):
    """logout() should remove the current cloud profile."""
    # First create a profile
    set_cloud_profile(sample_api_key, sample_workspace)
    assert mock_profiles_path.exists()

    # Then remove it
    logout()
    assert not mock_profiles_path.exists()


async def test_get_cloud_urls_or_login_with_valid_profile(mock_profiles_path: Path):
    """get_cloud_urls_or_login() should return correct URLs and API key when profile exists."""
    profile = {
        "profiles": {
            "prefect-cloud": {
                "PREFECT_API_KEY": "test_key",
                "PREFECT_API_URL": "https://api.prefect.cloud/api/accounts/123/workspaces/456",
            }
        }
    }
    mock_profiles_path.parent.mkdir(parents=True, exist_ok=True)
    mock_profiles_path.write_text(toml.dumps(profile))

    ui_url, api_url, api_key = await get_cloud_urls_or_login()

    assert ui_url == "https://app.prefect.cloud/account/123/workspace/456"
    assert api_url == "https://api.prefect.cloud/api/accounts/123/workspaces/456"
    assert api_key == "test_key"


async def test_get_cloud_urls_or_login_with_no_profile_successful_login(
    mock_profiles_path: Path, sample_api_key: str, sample_workspace: Workspace
):
    """get_cloud_urls_or_login() should attempt login when no profile exists."""
    with patch("prefect_cloud.auth.login") as mock_login:
        mock_login.side_effect = lambda: set_cloud_profile(
            sample_api_key, sample_workspace
        )

        ui_url, api_url, api_key = await get_cloud_urls_or_login()

        mock_login.assert_called_once()
        assert (
            ui_url
            == "https://app.prefect.cloud/account/11111111-1234-5678-1234-567812345678/workspace/22222222-1234-5678-1234-567812345678"
        )
        assert api_url == sample_workspace.api_url
        assert api_key == sample_api_key


async def test_get_cloud_urls_or_login_with_no_profile_failed_login(
    mock_profiles_path: Path,
):
    """get_cloud_urls_or_login() should raise error when login fails."""
    with patch("prefect_cloud.auth.login") as mock_login:
        mock_login.return_value = None

        with pytest.raises(ValueError, match="No cloud profile found"):
            await get_cloud_urls_or_login()

        mock_login.assert_called_once()


def test_get_cloud_urls_without_login_with_no_profile(mock_profiles_path: Path):
    """get_cloud_urls_without_login() should return None values when no profile exists."""
    ui_url, api_url, api_key = get_cloud_urls_without_login()
    assert ui_url is None
    assert api_url is None
    assert api_key is None


def test_get_cloud_urls_without_login_with_profile_missing_api_url(
    mock_profiles_path: Path,
):
    """get_cloud_urls_without_login() should return None values when profile has no API URL."""
    profile = {
        "profiles": {
            "prefect-cloud": {
                "PREFECT_API_KEY": "test_key",
            }
        }
    }
    mock_profiles_path.parent.mkdir(parents=True, exist_ok=True)
    mock_profiles_path.write_text(toml.dumps(profile))

    ui_url, api_url, api_key = get_cloud_urls_without_login()
    assert ui_url is None
    assert api_url is None
    assert api_key is None


def test_get_cloud_urls_without_login_with_profile_missing_api_key(
    mock_profiles_path: Path,
):
    """get_cloud_urls_without_login() should return None values when profile has no API key."""
    profile = {
        "profiles": {
            "prefect-cloud": {
                "PREFECT_API_URL": "https://api.prefect.cloud/api/accounts/123/workspaces/456",
            }
        }
    }
    mock_profiles_path.parent.mkdir(parents=True, exist_ok=True)
    mock_profiles_path.write_text(toml.dumps(profile))

    ui_url, api_url, api_key = get_cloud_urls_without_login()
    assert ui_url is None
    assert api_url is None
    assert api_key is None


@pytest.mark.parametrize(
    "api_url,expected_ui_url",
    [
        (
            "https://api.prefect.cloud/api/accounts/123/workspaces/456",
            "https://app.prefect.cloud/account/123/workspace/456",
        ),
        (
            "https://api.stg.prefect.dev/api/accounts/123/workspaces/456",
            "https://app.stg.prefect.dev/account/123/workspace/456",
        ),
        (
            "http://localhost:4200/api/accounts/123/workspaces/456",
            "http://localhost:4200/account/123/workspace/456",
        ),
    ],
)
async def test_get_cloud_urls_or_login_url_transformations(
    mock_profiles_path: Path, api_url: str, expected_ui_url: str
):
    """get_cloud_urls_or_login() should correctly transform API URLs to UI URLs."""
    profile = {
        "profiles": {
            "prefect-cloud": {
                "PREFECT_API_KEY": "test_key",
                "PREFECT_API_URL": api_url,
            }
        }
    }
    mock_profiles_path.parent.mkdir(parents=True, exist_ok=True)
    mock_profiles_path.write_text(toml.dumps(profile))

    ui_url, _, _ = await get_cloud_urls_or_login()
    assert ui_url == expected_ui_url
