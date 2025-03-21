"""
Prefect-specific exceptions.
"""

from typing import Any, Optional


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
