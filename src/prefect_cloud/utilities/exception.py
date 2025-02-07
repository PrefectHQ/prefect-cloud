"""
Prefect-specific exceptions.
"""

from typing import Any, Optional

from httpx._exceptions import HTTPStatusError
from typing_extensions import Self


class ObjectNotFound(Exception):
    """
    Raised when the client receives a 404 (not found) from the API.
    """

    def __init__(
        self,
        http_exc: Exception,
        help_message: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.http_exc = http_exc
        self.help_message = help_message
        super().__init__(help_message, *args, **kwargs)

    def __str__(self) -> str:
        return self.help_message or super().__str__()


class ObjectAlreadyExists(Exception):
    """
    Raised when the client receives a 409 (conflict) from the API.
    """

    def __init__(self, http_exc: Exception, *args: Any, **kwargs: Any) -> None:
        self.http_exc = http_exc
        super().__init__(*args, **kwargs)


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


class PrefectImportError(ImportError):
    """
    An error raised when a Prefect object cannot be imported due to a move or removal.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
