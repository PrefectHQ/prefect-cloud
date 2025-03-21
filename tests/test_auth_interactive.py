from unittest.mock import MagicMock, Mock

import httpx
import pytest
from respx import Router

from prefect_cloud.auth import (
    login_interactively,
    login_server,
    LoginError,
)


def test_login_server_handles_cors_preflight():
    """login_server should handle CORS preflight requests."""
    with login_server() as callback_ctx:
        response = httpx.options(callback_ctx.url)
        assert response.status_code == 200
        assert response.headers["Access-Control-Allow-Origin"] == "*"
        assert response.headers["Access-Control-Allow-Headers"] == "*"
        assert response.headers["Access-Control-Allow-Methods"] == "GET, POST, OPTIONS"


@pytest.fixture
def mock_webbrowser(monkeypatch: pytest.MonkeyPatch):
    """Mock the webbrowser module to prevent browser opening during tests."""
    mock = MagicMock()
    monkeypatch.setattr("webbrowser.open_new_tab", mock)
    return mock


def test_interactive_login_handles_get_callback(
    cloud_api: Router, mock_webbrowser: Mock
):
    """login_interactively() should handle the callback from the Cloud UI."""
    test_api_key = "test_key_123"

    def simulate_callback(uri):
        parsed_uri = httpx.URL(uri)
        callback_url = parsed_uri.params.get("callback")
        response = httpx.get(callback_url, params={"key": test_api_key})
        assert response.status_code == 200

    mock_webbrowser.side_effect = simulate_callback

    result = login_interactively()

    assert result == test_api_key


def test_interactive_login_handles_missing_api_key_get(mock_webbrowser: Mock):
    """login_server should handle requests without an API key."""

    def simulate_callback(uri):
        parsed_uri = httpx.URL(uri)
        callback_url = parsed_uri.params.get("callback")
        response = httpx.get(callback_url, params={})
        assert response.status_code == 200

    mock_webbrowser.side_effect = simulate_callback

    result = login_interactively()
    assert result is None


def test_interactive_login_handles_post_callback_success(
    cloud_api: Router, mock_webbrowser: Mock
):
    """login_interactively() should handle the callback from the Cloud UI."""
    test_api_key = "test_key_123"

    def simulate_callback(uri):
        parsed_uri = httpx.URL(uri)
        callback_url = parsed_uri.params.get("callback")
        response = httpx.post(callback_url + "/success", json={"api_key": test_api_key})
        assert response.status_code == 200

    mock_webbrowser.side_effect = simulate_callback

    result = login_interactively()
    assert result == test_api_key


def test_interactive_login_handles_post_callback_cancelled(
    cloud_api: Router, mock_webbrowser: Mock
):
    """login_interactively() should handle the callback from the Cloud UI."""

    def simulate_callback(uri):
        parsed_uri = httpx.URL(uri)
        callback_url = parsed_uri.params.get("callback")
        response = httpx.post(callback_url + "/failure")
        assert response.status_code == 200

    mock_webbrowser.side_effect = simulate_callback
    with pytest.raises(LoginError):
        login_interactively()


def test_interactive_login_handles_missing_api_key_post(mock_webbrowser: Mock):
    """login_server should handle requests without an API key."""

    def simulate_callback(uri):
        parsed_uri = httpx.URL(uri)
        callback_url = parsed_uri.params.get("callback")
        response = httpx.post(
            callback_url,
            json={"not_api_key": "test"},
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 200

    mock_webbrowser.side_effect = simulate_callback

    result = login_interactively()
    assert result is None
