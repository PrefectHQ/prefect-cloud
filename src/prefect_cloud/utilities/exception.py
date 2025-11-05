"""
Prefect-specific exceptions.
"""

from typing import Any, Optional

from httpx import HTTPStatusError


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


class ForbiddenError(Exception):
    """
    Raised when a request returns a 403 Forbidden status.

    This exception automatically extracts and includes the detail message
    from the API response to provide context about why the request was forbidden.
    """

    def __init__(self, http_exc: HTTPStatusError, *args: Any, **kwargs: Any) -> None:
        self.http_exc = http_exc

        message = "Access forbidden"
        try:
            detail = http_exc.response.json().get("detail")
            if detail:
                message = detail
        except (ValueError, KeyError):
            message = f"Access forbidden: {http_exc.response.text or http_exc.response.reason_phrase}"

        super().__init__(message, *args, **kwargs)
