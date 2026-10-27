import json
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest
import respx
from httpx import Response

from prefect_cloud.client import PrefectCloudClient
from prefect_cloud.github import install_github_app_interactively

PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/123/workspaces/456"
STATE_TOKEN_URL = (
    "https://api.prefect.cloud/api/accounts/123/integrations/github/state-token"
)


@pytest.fixture
def client() -> PrefectCloudClient:
    return PrefectCloudClient(api_url=PREFECT_API_URL, api_key="test_key")


@pytest.fixture(autouse=True)
def mock_account_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "prefect_cloud.github.get_account_id",
        AsyncMock(return_value=UUID("00000000-0000-0000-0000-000000000123")),
    )


@pytest.fixture(autouse=True)
def default_cloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLOUD_ENV", raising=False)


def mock_state_token_endpoint(
    respx_mock: respx.Router, response_body: dict[str, str]
) -> list[str]:
    """Mock the state-token endpoint and record the local callback URL sent to it."""
    local_callback_urls: list[str] = []

    def respond(request: httpx.Request) -> Response:
        redirect_url = httpx.URL(json.loads(request.content))
        local_callback_urls.append(redirect_url.params["callback"])
        return Response(200, json=response_body)

    respx_mock.post(STATE_TOKEN_URL).mock(side_effect=respond)
    return local_callback_urls


@pytest.fixture
def mock_webbrowser(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock()
    monkeypatch.setattr("webbrowser.open_new_tab", mock)
    return mock


async def test_opens_authorize_url_when_server_provides_one(
    client: PrefectCloudClient, respx_mock: respx.Router, mock_webbrowser: MagicMock
):
    authorize_url = "https://github.com/login/oauth/authorize?client_id=abc&state=tok"
    local_callback_urls = mock_state_token_endpoint(
        respx_mock, {"state_token": "tok", "authorize_url": authorize_url}
    )
    mock_webbrowser.side_effect = lambda url: httpx.get(local_callback_urls[0])

    await install_github_app_interactively(client)

    mock_webbrowser.assert_called_once_with(authorize_url)


async def test_opens_install_url_when_server_omits_authorize_url(
    client: PrefectCloudClient, respx_mock: respx.Router, mock_webbrowser: MagicMock
):
    local_callback_urls = mock_state_token_endpoint(respx_mock, {"state_token": "tok"})
    mock_webbrowser.side_effect = lambda url: httpx.get(local_callback_urls[0])

    await install_github_app_interactively(client)

    mock_webbrowser.assert_called_once_with(
        "https://github.com/apps/prefect-cloud/installations/new?state=tok"
    )
