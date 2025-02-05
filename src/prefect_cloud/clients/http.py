import copy
import sys
import uuid
from collections.abc import Awaitable
from datetime import datetime, timezone
from logging import Logger
from typing import TYPE_CHECKING, Any, Callable, Optional

import anyio
import httpx
from httpx import HTTPStatusError, Request, Response
from starlette import status
from typing_extensions import Self
from prefect_cloud.schemas.objects import CsrfToken
from prefect_cloud.utilities.math import (
    bounded_poisson_interval,
    clamped_poisson_interval,
)

from prefect.logging import get_logger

SERVER_API_VERSION = "0.8.4"
PREFECT_CLIENT_MAX_RETRIES = 5
PREFECT_CLIENT_RETRY_EXTRA_CODES = []
PREFECT_CLIENT_RETRY_JITTER_FACTOR = 0.2

logger: Logger = get_logger("client")


class PrefectHTTPStatusError(HTTPStatusError):
    """
    Raised when client receives a `Response` that contains an HTTPStatusError.

    Used to include API error details in the error messages that the client provides users.
    """

    @classmethod
    def from_httpx_error(cls: type[Self], httpx_error: HTTPStatusError) -> Self:
        """
        Generate a `PrefectHTTPStatusError` from an `httpx.HTTPStatusError`.
        """
        try:
            details = httpx_error.response.json()
        except Exception:
            details = None

        error_message, *more_info = str(httpx_error).split("\n")

        if details:
            message_components = [error_message, f"Response: {details}", *more_info]
        else:
            message_components = [error_message, *more_info]

        new_message = "\n".join(message_components)

        return cls(
            new_message, request=httpx_error.request, response=httpx_error.response
        )


class PrefectResponse(httpx.Response):
    """
    A Prefect wrapper for the `httpx.Response` class.

    Provides more informative error messages.
    """

    def raise_for_status(self) -> Response:
        """
        Raise an exception if the response contains an HTTPStatusError.

        The `PrefectHTTPStatusError` contains useful additional information that
        is not contained in the `HTTPStatusError`.
        """
        try:
            return super().raise_for_status()
        except HTTPStatusError as exc:
            raise PrefectHTTPStatusError.from_httpx_error(exc) from exc.__cause__

    @classmethod
    def from_httpx_response(cls: type[Self], response: httpx.Response) -> Response:
        """
        Create a `PrefectResponse` from an `httpx.Response`.

        By changing the `__class__` attribute of the Response, we change the method
        resolution order to look for methods defined in PrefectResponse, while leaving
        everything else about the original Response instance intact.
        """
        new_response = copy.copy(response)
        new_response.__class__ = cls
        return new_response


class PrefectHttpxAsyncClient(httpx.AsyncClient):
    """
    A Prefect wrapper for the async httpx client with support for retry-after headers
    for the provided status codes (typically 429, 502 and 503).

    Additionally, this client will always call `raise_for_status` on responses.

    For more details on rate limit headers, see:
    [Configuring Cloudflare Rate Limiting](https://support.cloudflare.com/hc/en-us/articles/115001635128-Configuring-Rate-Limiting-from-UI)
    """

    def __init__(
        self,
        *args: Any,
        enable_csrf_support: bool = False,
        raise_on_all_errors: bool = True,
        **kwargs: Any,
    ):
        self.enable_csrf_support: bool = enable_csrf_support
        self.csrf_token: Optional[str] = None
        self.csrf_token_expiration: Optional[datetime] = None
        self.csrf_client_id: uuid.UUID = uuid.uuid4()
        self.raise_on_all_errors: bool = raise_on_all_errors

        super().__init__(*args, **kwargs)

        # user_agent = (
        #     f"prefect/{prefect.__version__} (API {SERVER_API_VERSION})"
        # )
        # self.headers["User-Agent"] = user_agent

    async def _send_with_retry(
        self,
        request: Request,
        send: Callable[[Request], Awaitable[Response]],
        send_args: tuple[Any, ...],
        send_kwargs: dict[str, Any],
        retry_codes: set[int] = set(),
        retry_exceptions: tuple[type[Exception], ...] = tuple(),
    ):
        """
        Send a request and retry it if it fails.

        Sends the provided request and retries it up to PREFECT_CLIENT_MAX_RETRIES times
        if the request either raises an exception listed in `retry_exceptions` or
        receives a response with a status code listed in `retry_codes`.

        Retries will be delayed based on either the retry header (preferred) or
        exponential backoff if a retry header is not provided.
        """
        try_count = 0
        response = None

        if TYPE_CHECKING:
            # older httpx versions type method as str | bytes | Unknown
            # but in reality it is always a string.
            assert isinstance(request.method, str)  # type: ignore

        is_change_request = request.method.lower() in {"post", "put", "patch", "delete"}

        if self.enable_csrf_support and is_change_request:
            await self._add_csrf_headers(request=request)

        while try_count <= PREFECT_CLIENT_MAX_RETRIES:
            try_count += 1
            retry_seconds = None
            exc_info = None

            try:
                response = await send(request, *send_args, **send_kwargs)
            except retry_exceptions:  # type: ignore
                if try_count > PREFECT_CLIENT_MAX_RETRIES:
                    raise
                # Otherwise, we will ignore this error but capture the info for logging
                exc_info = sys.exc_info()
            else:
                # We got a response; check if it's a CSRF error, otherwise
                # return immediately if it is not retryable
                if (
                    response.status_code == status.HTTP_403_FORBIDDEN
                    and "Invalid CSRF token" in response.text
                ):
                    # We got a CSRF error, clear the token and try again
                    self.csrf_token = None
                    await self._add_csrf_headers(request)
                elif response.status_code not in retry_codes:
                    return response

                if "Retry-After" in response.headers:
                    retry_seconds = float(response.headers["Retry-After"])

            # Use an exponential back-off if not set in a header
            if retry_seconds is None:
                retry_seconds = 2**try_count

            # Add jitter
            jitter_factor = PREFECT_CLIENT_RETRY_JITTER_FACTOR
            if retry_seconds > 0 and jitter_factor > 0:
                if response is not None and "Retry-After" in response.headers:
                    # Always wait for _at least_ retry seconds if requested by the API
                    retry_seconds = bounded_poisson_interval(
                        retry_seconds, retry_seconds * (1 + jitter_factor)
                    )
                else:
                    # Otherwise, use a symmetrical jitter
                    retry_seconds = clamped_poisson_interval(
                        retry_seconds, jitter_factor
                    )

            logger.debug(
                (
                    "Encountered retryable exception during request. "
                    if exc_info
                    else (
                        "Received response with retryable status code"
                        f" {response.status_code if response else 'unknown'}. "
                    )
                )
                + f"Another attempt will be made in {retry_seconds}s. "
                "This is attempt"
                f" {try_count}/{PREFECT_CLIENT_MAX_RETRIES + 1}.",
                exc_info=exc_info,
            )
            await anyio.sleep(retry_seconds)

        assert response is not None, (
            "Retry handling ended without response or exception"
        )

        # We ran out of retries, return the failed response
        return response

    async def send(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        """
        Send a request with automatic retry behavior for the following status codes:

        - 403 Forbidden, if the request failed due to CSRF protection
        - 408 Request Timeout
        - 429 CloudFlare-style rate limiting
        - 502 Bad Gateway
        - 503 Service unavailable
        - Any additional status codes provided in `PREFECT_CLIENT_RETRY_EXTRA_CODES`
        """

        super_send = super().send
        response = await self._send_with_retry(
            request=request,
            send=super_send,
            send_args=args,
            send_kwargs=kwargs,
            retry_codes={
                status.HTTP_429_TOO_MANY_REQUESTS,
                status.HTTP_503_SERVICE_UNAVAILABLE,
                status.HTTP_502_BAD_GATEWAY,
                status.HTTP_408_REQUEST_TIMEOUT,
                *PREFECT_CLIENT_RETRY_EXTRA_CODES,
            },
            retry_exceptions=(
                httpx.ReadTimeout,
                httpx.PoolTimeout,
                httpx.ConnectTimeout,
                # `ConnectionResetError` when reading socket raises as a `ReadError`
                httpx.ReadError,
                # Sockets can be closed during writes resulting in a `WriteError`
                httpx.WriteError,
                # Uvicorn bug, see https://github.com/PrefectHQ/prefect/issues/7512
                httpx.RemoteProtocolError,
                # HTTP2 bug, see https://github.com/PrefectHQ/prefect/issues/7442
                httpx.LocalProtocolError,
            ),
        )

        # Convert to a Prefect response to add nicer errors messages
        response = PrefectResponse.from_httpx_response(response)

        if self.raise_on_all_errors:
            response.raise_for_status()

        return response

    async def _add_csrf_headers(self, request: Request):
        now = datetime.now(timezone.utc)

        if not self.enable_csrf_support:
            return

        if not self.csrf_token or (
            self.csrf_token_expiration and now > self.csrf_token_expiration
        ):
            token_request = self.build_request(
                "GET", f"/csrf-token?client={self.csrf_client_id}"
            )

            try:
                token_response = await self.send(token_request)
            except PrefectHTTPStatusError as exc:
                old_server = exc.response.status_code == status.HTTP_404_NOT_FOUND
                unconfigured_server = (
                    exc.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
                    and "CSRF protection is disabled." in exc.response.text
                )

                if old_server or unconfigured_server:
                    # The token endpoint is either unavailable, suggesting an
                    # older server, or CSRF protection is disabled. In either
                    # case we should disable CSRF support.
                    self.enable_csrf_support = False
                    return

                raise

            token: CsrfToken = CsrfToken.model_validate(token_response.json())
            self.csrf_token = token.token
            self.csrf_token_expiration = token.expiration

        request.headers["Prefect-Csrf-Token"] = self.csrf_token
        request.headers["Prefect-Csrf-Client"] = str(self.csrf_client_id)
