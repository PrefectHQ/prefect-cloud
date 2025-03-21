from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Queue
from typing import Any, Generator, TypeVar, Type
from urllib.parse import parse_qs, urlparse
import json
import socket
import threading

T = TypeVar("T")


@dataclass
class CallbackContext:
    """Container for callback server context."""

    url: str
    queue: Queue[Any]

    def wait_for_callback(self) -> Any:
        """Wait for a callback result."""
        result = self.queue.get()

        # re-raise exceptions
        if isinstance(result, Exception):
            raise result

        return result


class CallbackServerHandler(BaseHTTPRequestHandler):
    """Base handler for callback server requests."""

    result_queue: Queue[Any]

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def add_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

    def do_OPTIONS(self) -> None:
        self.send_response(200)
        self.add_cors_headers()
        self.end_headers()

    def do_GET(self) -> None:
        parts = urlparse(self.path)
        query_params = parse_qs(parts.query)
        path = parts.path
        try:
            result = self.process_get(path, query_params)
        except Exception as e:
            result = e

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"""
            <script>
            if (window.opener) {
                window.opener.postMessage('callback-complete', '*')
            }
            </script>
            <p>You can close this window.</p>
        """)
        self.result_queue.put(result)

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        data: dict[str, Any] = json.loads(body) if body else {}
        path = urlparse(self.path).path
        try:
            result = self.process_post(path, data)
        except Exception as e:
            result = e

        self.send_response(200)
        self.add_cors_headers()
        self.end_headers()
        self.wfile.write(b"{}")
        self.result_queue.put(result)

    def process_get(self, path: str, query_params: dict[str, list[str]]) -> Any:
        """
        Process GET request and return a result to be put in the queue.

        Exceptions raised here will be re-raised in the main thread.
        """
        return None

    def process_post(self, path: str, data: dict[str, Any]) -> Any:
        """
        Process POST request and return a result to be put in the queue.

        Exceptions raised here will be re-raised in the main thread.
        """
        return None


class CallbackServer:
    """
    Server that handles callbacks from external services.

    To customize behavior, subclass CallbackServerHandler and pass it to the constructor.
    """

    def __init__(
        self, handler_class: Type[CallbackServerHandler] = CallbackServerHandler
    ):
        """
        Initialize the callback server.

        Args:
            handler_class: The request handler class to use. Defaults to CallbackServerHandler.
        """
        self.handler_class = handler_class
        self.server = None
        self.thread = None

    @contextmanager
    def run(self) -> Generator[CallbackContext, None, None]:
        """
        Run the server and yield a CallbackContext.

        Yields:
            CallbackContext
        """
        result_queue: Queue[Any] = Queue()

        handler = type(
            "ConfiguredHandler", (self.handler_class,), {"result_queue": result_queue}
        )

        with socket.socket() as sock:
            sock.bind(("", 0))
            port = sock.getsockname()[1]

        self.server = HTTPServer(("localhost", port), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        try:
            yield CallbackContext(url=f"http://localhost:{port}", queue=result_queue)
        finally:
            if self.server:
                self.server.shutdown()
                self.server.server_close()


@contextmanager
def callback_server(
    handler_class: Type[CallbackServerHandler] = CallbackServerHandler,
) -> Generator[CallbackContext, None, None]:
    """
    Generic callback server context manager.

    Yields:
        CallbackContext: Container with the server URL and result queue.
    """
    server = CallbackServer(handler_class=handler_class)
    with server.run() as context:
        yield context
