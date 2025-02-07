from __future__ import annotations

import datetime

from typing import TYPE_CHECKING, Any, Optional, Type

from opentelemetry import propagate
from pendulum.duration import Duration as PendulumDuration
from prefect.client.schemas import State as State
from prefect.client.schemas import StateDetails, StateType

from prefect.types import DateTime

if TYPE_CHECKING:
    from prefect.results import R


RESULT_READ_MAXIMUM_ATTEMPTS = 10
RESULT_READ_RETRY_DELAY = 0.25


def _traced(cls: Type["State[R]"], **kwargs: Any) -> "State[R]":
    state_details = StateDetails.model_validate(kwargs.pop("state_details", {}))

    carrier = {}
    propagate.inject(carrier)
    state_details.traceparent = carrier.get("traceparent")

    return cls(**kwargs, state_details=state_details)


def Scheduled(
    cls: Type["State[R]"] = State,
    scheduled_time: Optional[datetime.datetime] = None,
    **kwargs: Any,
) -> "State[R]":
    """Convenience function for creating `Scheduled` states.

    Returns:
        State: a Scheduled state
    """
    state_details = StateDetails.model_validate(kwargs.pop("state_details", {}))
    if scheduled_time is None:
        scheduled_time = DateTime.now("UTC")
    elif state_details.scheduled_time:
        raise ValueError("An extra scheduled_time was provided in state_details")
    state_details.scheduled_time = scheduled_time

    return _traced(cls, type=StateType.SCHEDULED, state_details=state_details, **kwargs)


def Completed(cls: Type["State[R]"] = State, **kwargs: Any) -> "State[R]":
    """Convenience function for creating `Completed` states.

    Returns:
        State: a Completed state
    """

    return _traced(cls, type=StateType.COMPLETED, **kwargs)


def Running(cls: Type["State[R]"] = State, **kwargs: Any) -> "State[R]":
    """Convenience function for creating `Running` states.

    Returns:
        State: a Running state
    """
    return _traced(cls, type=StateType.RUNNING, **kwargs)


def Failed(cls: Type["State[R]"] = State, **kwargs: Any) -> "State[R]":
    """Convenience function for creating `Failed` states.

    Returns:
        State: a Failed state
    """
    return _traced(cls, type=StateType.FAILED, **kwargs)


def Crashed(cls: Type["State[R]"] = State, **kwargs: Any) -> "State[R]":
    """Convenience function for creating `Crashed` states.

    Returns:
        State: a Crashed state
    """
    return _traced(cls, type=StateType.CRASHED, **kwargs)


def Cancelling(cls: Type["State[R]"] = State, **kwargs: Any) -> "State[R]":
    """Convenience function for creating `Cancelling` states.

    Returns:
        State: a Cancelling state
    """
    return _traced(cls, type=StateType.CANCELLING, **kwargs)


def Cancelled(cls: Type["State[R]"] = State, **kwargs: Any) -> "State[R]":
    """Convenience function for creating `Cancelled` states.

    Returns:
        State: a Cancelled state
    """
    return _traced(cls, type=StateType.CANCELLED, **kwargs)


def Pending(cls: Type["State[R]"] = State, **kwargs: Any) -> "State[R]":
    """Convenience function for creating `Pending` states.

    Returns:
        State: a Pending state
    """
    return _traced(cls, type=StateType.PENDING, **kwargs)


def Paused(
    cls: Type["State[R]"] = State,
    timeout_seconds: Optional[int] = None,
    pause_expiration_time: Optional[datetime.datetime] = None,
    reschedule: bool = False,
    pause_key: Optional[str] = None,
    **kwargs: Any,
) -> "State[R]":
    """Convenience function for creating `Paused` states.

    Returns:
        State: a Paused state
    """
    state_details = StateDetails.model_validate(kwargs.pop("state_details", {}))

    if state_details.pause_timeout:
        raise ValueError("An extra pause timeout was provided in state_details")

    if pause_expiration_time is not None and timeout_seconds is not None:
        raise ValueError(
            "Cannot supply both a pause_expiration_time and timeout_seconds"
        )

    if pause_expiration_time is None and timeout_seconds is None:
        pass
    else:
        state_details.pause_timeout = (
            DateTime.instance(pause_expiration_time)
            if pause_expiration_time
            else DateTime.now("UTC") + PendulumDuration(seconds=timeout_seconds or 0)
        )

    state_details.pause_reschedule = reschedule
    state_details.pause_key = pause_key

    return _traced(cls, type=StateType.PAUSED, state_details=state_details, **kwargs)


def Suspended(
    cls: Type["State[R]"] = State,
    timeout_seconds: Optional[int] = None,
    pause_expiration_time: Optional[datetime.datetime] = None,
    pause_key: Optional[str] = None,
    **kwargs: Any,
) -> "State[R]":
    """Convenience function for creating `Suspended` states.

    Returns:
        State: a Suspended state
    """
    return Paused(
        cls=cls,
        name="Suspended",
        reschedule=True,
        timeout_seconds=timeout_seconds,
        pause_expiration_time=pause_expiration_time,
        pause_key=pause_key,
        **kwargs,
    )


def AwaitingRetry(
    cls: Type["State[R]"] = State,
    scheduled_time: Optional[datetime.datetime] = None,
    **kwargs: Any,
) -> "State[R]":
    """Convenience function for creating `AwaitingRetry` states.

    Returns:
        State: a AwaitingRetry state
    """
    return Scheduled(
        cls=cls, scheduled_time=scheduled_time, name="AwaitingRetry", **kwargs
    )


def AwaitingConcurrencySlot(
    cls: Type["State[R]"] = State,
    scheduled_time: Optional[datetime.datetime] = None,
    **kwargs: Any,
) -> "State[R]":
    """Convenience function for creating `AwaitingConcurrencySlot` states.

    Returns:
        State: a AwaitingConcurrencySlot state
    """
    return Scheduled(
        cls=cls, scheduled_time=scheduled_time, name="AwaitingConcurrencySlot", **kwargs
    )


def Retrying(cls: Type["State[R]"] = State, **kwargs: Any) -> "State[R]":
    """Convenience function for creating `Retrying` states.

    Returns:
        State: a Retrying state
    """
    return _traced(cls, type=StateType.RUNNING, name="Retrying", **kwargs)


def Late(
    cls: Type["State[R]"] = State,
    scheduled_time: Optional[datetime.datetime] = None,
    **kwargs: Any,
) -> "State[R]":
    """Convenience function for creating `Late` states.

    Returns:
        State: a Late state
    """
    return Scheduled(cls=cls, scheduled_time=scheduled_time, name="Late", **kwargs)
