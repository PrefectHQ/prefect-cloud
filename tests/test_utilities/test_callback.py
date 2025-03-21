from queue import Queue

import httpx
from typing import Any, Dict

import pytest

from prefect_cloud.utilities.callback import (
    CallbackServerHandler,
    callback_server,
)


def test_callback_server_handles_cors_preflight():
    """Callback server should handle CORS preflight requests."""
    with callback_server() as callback_ctx:
        response = httpx.options(callback_ctx.url)
        assert response.status_code == 200
        assert response.headers["Access-Control-Allow-Origin"] == "*"
        assert response.headers["Access-Control-Allow-Headers"] == "*"
        assert response.headers["Access-Control-Allow-Methods"] == "GET, POST, OPTIONS"


def test_callback_server_basic_functionality():
    """Callback server should start and provide a valid URL."""
    with callback_server() as callback_ctx:
        assert callback_ctx.url.startswith("http://localhost:")
        assert isinstance(callback_ctx.queue, Queue)
        assert callable(callback_ctx.wait_for_callback)


def test_callback_server_custom_handler():
    """Callback server should support custom handlers."""

    class TestHandler(CallbackServerHandler):
        def process_get(self, path: str, query_params: Dict[str, list[str]]) -> str:
            return "test_get_handler"

        def process_post(self, path: str, data: Dict[str, Any]) -> str:
            return "test_post_handler"

    with callback_server(handler_class=TestHandler) as callback_ctx:
        response = httpx.get(callback_ctx.url)
        assert response.status_code == 200

        result = callback_ctx.wait_for_callback()
        assert result == "test_get_handler"

        response = httpx.post(callback_ctx.url, json={})
        assert response.status_code == 200

        result = callback_ctx.wait_for_callback()
        assert result == "test_post_handler"


def test_callback_server_get_parameters():
    """Callback server should extract parameters from GET request."""

    class TestGetHandler(CallbackServerHandler):
        def process_get(
            self, path: str, query_params: Dict[str, list[str]]
        ) -> Dict[str, str]:
            return {
                "param1": query_params.get("param1", [""])[0],
                "param2": query_params.get("param2", [""])[0],
            }

    with callback_server(handler_class=TestGetHandler) as callback_ctx:
        response = httpx.get(
            callback_ctx.url, params={"param1": "value1", "param2": "value2"}
        )
        assert response.status_code == 200

        result = callback_ctx.wait_for_callback()
        assert result == {"param1": "value1", "param2": "value2"}


def test_callback_server_post_json():
    """Callback server should parse JSON data from POST request."""

    class TestPostHandler(CallbackServerHandler):
        def process_post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
            return data

    with callback_server(handler_class=TestPostHandler) as callback_ctx:
        test_data = {"key1": "value1", "key2": 42, "nested": {"foo": "bar"}}
        response = httpx.post(callback_ctx.url, json=test_data)
        assert response.status_code == 200

        result = callback_ctx.wait_for_callback()
        assert result == test_data


def test_callback_server_wait_for_callback():
    """wait_for_callback() should return the first result from the queue."""

    class TestDelayedHandler(CallbackServerHandler):
        def process_get(self, path: str, query_params: Dict[str, list[str]]) -> str:
            return "result"

    with callback_server(handler_class=TestDelayedHandler) as callback_ctx:
        httpx.get(callback_ctx.url)

        result = callback_ctx.wait_for_callback()
        assert result == "result"


def test_callback_server_multiple_requests():
    """Callback server should handle multiple requests in sequence."""
    counter = 0

    class CountingHandler(CallbackServerHandler):
        def process_get(self, path: str, query_params: Dict[str, list[str]]) -> int:
            nonlocal counter
            counter += 1
            return counter

    with callback_server(handler_class=CountingHandler) as callback_ctx:
        # Send multiple GET requests
        for _ in range(3):
            httpx.get(callback_ctx.url)

        # Each request should increment the counter
        assert callback_ctx.wait_for_callback() == 1
        assert callback_ctx.wait_for_callback() == 2
        assert callback_ctx.wait_for_callback() == 3


def test_callback_server_html_response():
    """Callback server should return HTML with the expected script for closing the window."""
    with callback_server() as callback_ctx:
        response = httpx.get(callback_ctx.url)
        assert response.status_code == 200
        assert response.headers["Content-type"] == "text/html"

        assert "window.opener.postMessage('callback-complete', '*')" in response.text
        assert "You can close this window." in response.text


def test_callback_server_empty_post():
    """Callback server should handle empty POST requests."""
    with callback_server() as callback_ctx:
        response = httpx.post(callback_ctx.url)
        assert response.status_code == 200

        # Default handler returns None
        assert callback_ctx.wait_for_callback() is None


def test_callback_context_reraise_exceptions():
    """wait_for_callback() should reraise any exceptions returned by the handler."""

    class TestExceptionRaisedHandler(CallbackServerHandler):
        def process_get(self, path: str, query_params: Dict[str, list[str]]):
            raise ValueError("everything has get wrong!")

        def process_post(self, path: str, data: Dict[str, list[str]]):
            raise ValueError("everything has post wrong!")

    with callback_server(handler_class=TestExceptionRaisedHandler) as callback_ctx:
        with pytest.raises(ValueError, match="everything has get wrong!"):
            httpx.get(callback_ctx.url)
            callback_ctx.wait_for_callback()

        with pytest.raises(ValueError, match="everything has post wrong!"):
            httpx.post(callback_ctx.url)
            callback_ctx.wait_for_callback()
