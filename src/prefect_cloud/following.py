import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import Any, Iterable, Optional, cast
from uuid import UUID

from cachetools import TTLCache
from pydantic import BaseModel, Field, RootModel
from typing_extensions import Self
from websockets import Subprotocol
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedOK,
)

TERMINAL_FLOW_RUN_EVENTS = {
    "prefect.flow-run.Completed",
    "prefect.flow-run.Failed",
    "prefect.flow-run.Crashed",
}


class Log(BaseModel):
    id: UUID = Field()
    name: str = Field(default=..., description="The logger name.")
    level: int = Field(default=..., description="The log level.")
    message: str = Field(default=..., description="The log message.")
    timestamp: datetime = Field(default=..., description="The log timestamp.")
    flow_run_id: UUID | None = Field(
        default=None, description="The flow run ID associated with the log."
    )
    task_run_id: UUID | None = Field(
        default=None, description="The task run ID associated with the log."
    )
    worker_id: UUID | None = Field(
        default=None, description="The worker ID associated with the log."
    )


def _http_to_ws(url: str) -> str:
    """Convert HTTP/HTTPS URL to WebSocket URL"""
    return url.replace("https://", "wss://").replace("http://", "ws://").rstrip("/")


class LogsSubscriber:
    _websocket: Optional[ClientConnection]
    _filter: dict[str, Any]
    _seen_logs: TTLCache[UUID, bool]

    _api_url: str
    _api_key: str

    def __init__(
        self,
        api_url: str,
        api_key: str,
        filter: Optional[dict[str, Any]] = None,
        reconnection_attempts: int = 10,
    ):
        self._api_url = api_url
        self._api_key = api_key
        self._filter = filter or {}
        self._seen_logs = TTLCache(
            maxsize=10000, ttl=300
        )  # 5 minute TTL, 10k max items

        self._connect = connect(
            _http_to_ws(self._api_url + "/logs/out"),
            subprotocols=[Subprotocol("prefect")],
        )
        self._websocket = None
        self._reconnection_attempts = reconnection_attempts
        if self._reconnection_attempts < 0:
            raise ValueError("reconnection_attempts must be a non-negative integer")

    async def __aenter__(self) -> Self:
        # Don't handle any errors in the initial connection, because these are most
        # likely a permission or configuration issue that should propagate
        await self._reconnect()
        return self

    async def _reconnect(self) -> None:
        if self._websocket:
            self._websocket = None
            await self._connect.__aexit__(None, None, None)

        self._websocket = await self._connect.__aenter__()

        # make sure we have actually connected
        pong = await self._websocket.ping()
        await pong

        # Send authentication message - logs WebSocket requires auth handshake
        await self._websocket.send(json.dumps({"type": "auth", "token": self._api_key}))

        # Wait for auth response
        try:
            message = json.loads(await self._websocket.recv())
            assert message["type"] == "auth_success", message.get("reason", "")
        except AssertionError as e:
            raise Exception(
                "Unable to authenticate to the log stream. Please ensure the "
                "provided api_key or auth_token you are using is valid for this environment. "
                f"Reason: {e.args[0]}"
            )
        except ConnectionClosed as e:
            reason = getattr(e.rcvd, "reason", None)
            msg = "Unable to authenticate to the log stream. Please ensure the "
            msg += "provided api_key or auth_token you are using is valid for this environment. "
            msg += f"Reason: {reason}" if reason else ""
            raise Exception(msg) from e

        self._filter["timestamp"] = {
            "after_": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
            "before_": (datetime.now(timezone.utc) + timedelta(days=365)).isoformat(),
        }

        await self._websocket.send(
            json.dumps({"type": "filter", "filter": self._filter})
        )

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._websocket = None
        await self._connect.__aexit__(exc_type, exc_val, exc_tb)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Log:
        assert self._reconnection_attempts >= 0
        for i in range(self._reconnection_attempts + 1):  # pragma: no branch
            try:
                # If we're here and the websocket is None, then we've had a failure in a
                # previous reconnection attempt.
                #
                # Otherwise, after the first time through this loop, we're recovering
                # from a ConnectionClosed, so reconnect now.
                if not self._websocket or i > 0:
                    await self._reconnect()
                    assert self._websocket

                while True:
                    message = json.loads(await self._websocket.recv())
                    log = Log.model_validate(message["log"])

                    # Skip if we've already seen this log ID
                    if log.id in self._seen_logs:
                        continue

                    # Mark as seen
                    self._seen_logs[log.id] = True
                    return log

            except ConnectionClosedOK:
                raise StopAsyncIteration
            except ConnectionClosed:
                if i == self._reconnection_attempts:
                    # this was our final chance, raise the most recent error
                    raise

                if i > 2:
                    # let the first two attempts happen quickly in case this is just
                    # a standard load balancer timeout, but after that, just take a
                    # beat to let things come back around.
                    await asyncio.sleep(1)
        raise StopAsyncIteration


class Labelled(RootModel[dict[str, str]]):
    """An object defined by string labels and values"""

    def keys(self) -> Iterable[str]:
        return self.root.keys()

    def items(self) -> Iterable[tuple[str, str]]:
        return self.root.items()

    def __getitem__(self, label: str) -> str:
        return self.root[label]

    def __setitem__(self, label: str, value: str) -> str:
        self.root[label] = value
        return value

    def __contains__(self, key: str) -> bool:
        return key in self.root

    def get(self, label: str, default: str | None = None) -> str | None:
        return self.root.get(label, default)

    def as_label_value_array(self) -> list[dict[str, str]]:
        return [{"label": label, "value": value} for label, value in self.items()]

    def has_all_labels(self, labels: dict[str, str]) -> bool:
        return all(self.root.get(label) == value for label, value in labels.items())


class Resource(Labelled):
    """An observable business object of interest to the user"""

    @property
    def id(self) -> str:
        return self["prefect.resource.id"]

    @property
    def name(self) -> str | None:
        return self.get("prefect.resource.name")


class RelatedResource(Resource):
    """A Resource with a specific role in an Event"""

    @property
    def role(self) -> str:
        return self["prefect.resource.role"]


class Event(BaseModel):
    """The client-side view of an event that has happened to a Resource"""

    occurred: datetime = Field(
        description="When the event happened from the sender's perspective",
    )
    event: str = Field(
        description="The name of the event that happened",
        max_length=1024,
    )
    resource: Resource = Field(
        description="The primary Resource this event concerns",
    )
    related: list[RelatedResource] = Field(
        default_factory=lambda: cast(list[RelatedResource], []),
        description="A list of additional Resources involved in this event",
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="An open-ended set of data describing what happened",
    )
    id: UUID = Field(
        description="The client-provided identifier of this event",
    )
    follows: UUID | None = Field(
        None,
        description=(
            "The ID of an event that is known to have occurred prior to this one. "
            "If set, this may be used to establish a more precise ordering of causally-"
            "related events when they occur close enough together in time that the "
            "system may receive them out-of-order."
        ),
    )


class EventsSubscriber:
    _websocket: Optional[ClientConnection]
    _filter: dict[str, Any]
    _seen_events: TTLCache[UUID, bool]

    _api_url: str
    _api_key: str

    def __init__(
        self,
        api_url: str,
        api_key: str,
        filter: Optional[dict[str, Any]] = None,
        reconnection_attempts: int = 10,
    ):
        self._api_url = api_url
        self._api_key = api_key
        self._filter = filter or {}
        self._seen_events = TTLCache(
            maxsize=10000, ttl=300
        )  # 5 minute TTL, 10k max items

        self._connect = connect(
            _http_to_ws(self._api_url + "/events/out"),
            subprotocols=[Subprotocol("prefect")],
        )
        self._websocket = None
        self._reconnection_attempts = reconnection_attempts
        if self._reconnection_attempts < 0:
            raise ValueError("reconnection_attempts must be a non-negative integer")

    async def __aenter__(self) -> Self:
        # Don't handle any errors in the initial connection, because these are most
        # likely a permission or configuration issue that should propagate
        await self._reconnect()
        return self

    async def _reconnect(self) -> None:
        if self._websocket:
            self._websocket = None
            await self._connect.__aexit__(None, None, None)

        self._websocket = await self._connect.__aenter__()

        # make sure we have actually connected
        pong = await self._websocket.ping()
        await pong

        # Send authentication message - logs WebSocket requires auth handshake
        await self._websocket.send(json.dumps({"type": "auth", "token": self._api_key}))

        # Wait for auth response
        try:
            message = json.loads(await self._websocket.recv())
            assert message["type"] == "auth_success", message.get("reason", "")
        except AssertionError as e:
            raise Exception(
                "Unable to authenticate to the log stream. Please ensure the "
                "provided api_key or auth_token you are using is valid for this environment. "
                f"Reason: {e.args[0]}"
            )
        except ConnectionClosed as e:
            reason = getattr(e.rcvd, "reason", None)
            msg = "Unable to authenticate to the log stream. Please ensure the "
            msg += "provided api_key or auth_token you are using is valid for this environment. "
            msg += f"Reason: {reason}" if reason else ""
            raise Exception(msg) from e

        self._filter["occurred"] = {
            "since": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
            "until": (datetime.now(timezone.utc) + timedelta(days=365)).isoformat(),
        }

        await self._websocket.send(
            json.dumps({"type": "filter", "filter": self._filter})
        )

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._websocket = None
        await self._connect.__aexit__(exc_type, exc_val, exc_tb)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Event:
        assert self._reconnection_attempts >= 0
        for i in range(self._reconnection_attempts + 1):  # pragma: no branch
            try:
                # If we're here and the websocket is None, then we've had a failure in a
                # previous reconnection attempt.
                #
                # Otherwise, after the first time through this loop, we're recovering
                # from a ConnectionClosed, so reconnect now.
                if not self._websocket or i > 0:
                    await self._reconnect()
                    assert self._websocket

                while True:
                    message = json.loads(await self._websocket.recv())
                    event = Event.model_validate(message["event"])

                    # Skip if we've already seen this event ID
                    if event.id in self._seen_events:
                        continue

                    # Mark as seen
                    self._seen_events[event.id] = True
                    return event

            except ConnectionClosedOK:
                raise StopAsyncIteration
            except ConnectionClosed:
                if i == self._reconnection_attempts:
                    # this was our final chance, raise the most recent error
                    raise

                if i > 2:
                    # let the first two attempts happen quickly in case this is just
                    # a standard load balancer timeout, but after that, just take a
                    # beat to let things come back around.
                    await asyncio.sleep(1)
        raise StopAsyncIteration


class FlowRunSubscriber:
    _api_url: str
    _api_key: str
    _flow_run_id: UUID
    _queue: asyncio.Queue[Log | Event | None]
    _tasks: list[asyncio.Task[None]]
    _flow_completed: bool
    _straggler_timeout: int

    def __init__(
        self, api_url: str, api_key: str, flow_run_id: UUID, straggler_timeout: int = 3
    ):
        self._api_url = api_url
        self._api_key = api_key
        self._flow_run_id = flow_run_id
        self._logs_subscriber = LogsSubscriber(
            api_url, api_key, {"flow_run_id": {"any_": [str(flow_run_id)]}}
        )
        self._events_subscriber = EventsSubscriber(
            api_url,
            api_key,
            {"any_resource": {"id": [f"prefect.flow-run.{flow_run_id}"]}},
        )
        self._queue = asyncio.Queue()
        self._tasks = []
        self._flow_completed = False
        self._straggler_timeout = straggler_timeout

    async def __aenter__(self) -> Self:
        await self._logs_subscriber.__aenter__()
        await self._events_subscriber.__aenter__()

        self._tasks = [
            asyncio.create_task(self._consume_logs()),
            asyncio.create_task(self._consume_events()),
        ]

        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        for task in self._tasks:
            task.cancel()

        await asyncio.gather(*self._tasks, return_exceptions=True)

        await self._logs_subscriber.__aexit__(exc_type, exc_val, exc_tb)
        await self._events_subscriber.__aexit__(exc_type, exc_val, exc_tb)

    async def _consume_logs(self) -> None:
        try:
            async for log in self._logs_subscriber:
                await self._queue.put(log)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            await self._queue.put(None)

    async def _consume_events(self) -> None:
        try:
            async for event in self._events_subscriber:
                await self._queue.put(event)

                if (
                    event.event in TERMINAL_FLOW_RUN_EVENTS
                    and event.resource.id == f"prefect.flow-run.{self._flow_run_id}"
                ):
                    self._flow_completed = True
                    break  # Exit after terminal event
        except Exception:
            pass
        finally:
            await self._queue.put(None)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Log | Event:
        sentinels_received = 0

        while sentinels_received < len(self._tasks):
            if self._flow_completed:
                # After flow completion, timeout between messages (not total time)
                try:
                    item = await asyncio.wait_for(
                        self._queue.get(), timeout=self._straggler_timeout
                    )
                except asyncio.TimeoutError:
                    raise StopAsyncIteration
            else:
                # Before flow completion, wait indefinitely
                item = await self._queue.get()

            if item is None:
                sentinels_received += 1
                continue

            return item

        raise StopAsyncIteration


class FlowRunFormatter:
    """Handles formatting of logs and events for CLI display"""

    def __init__(self):
        # Track previous timestamp for incremental display
        self._last_timestamp_parts = [
            "",
            "",
            "",
            "",
        ]  # [hour, minute, second, millisecond]
        self._last_datetime = None

    def format_timestamp(self, dt: datetime) -> str:
        ms = dt.strftime("%f")[:3]  # Get first 3 digits of microseconds
        current_parts = [dt.strftime("%H"), dt.strftime("%M"), dt.strftime("%S"), ms]

        # Check if this timestamp is before the previous one (can happen with async streams)
        if self._last_datetime and dt < self._last_datetime:
            # Timestamp went backward, show full timestamp (still 12 chars)
            self._last_timestamp_parts = current_parts[:]
            self._last_datetime = dt
            return f"{current_parts[0]}:{current_parts[1]}:{current_parts[2]}.{current_parts[3]}"

        display_parts = []
        for i, (last, current) in enumerate(
            zip(self._last_timestamp_parts, current_parts)
        ):
            if current != last:
                # This part changed, show it and all subsequent parts
                display_parts = current_parts[i:]
                break
        else:
            # Nothing changed, show just milliseconds
            display_parts = [current_parts[3]]

        # Update our tracking
        self._last_timestamp_parts = current_parts[:]
        self._last_datetime = dt

        # Build display string with right alignment (12 characters total)
        if len(display_parts) == 4:  # H:M:S.ms
            timestamp_str = f"{display_parts[0]}:{display_parts[1]}:{display_parts[2]}.{display_parts[3]}"
        elif len(display_parts) == 3:  # M:S.ms
            timestamp_str = f":{display_parts[0]}:{display_parts[1]}.{display_parts[2]}"
        elif len(display_parts) == 2:  # S.ms
            timestamp_str = f":{display_parts[0]}.{display_parts[1]}"
        else:  # ms
            timestamp_str = f".{display_parts[0]}"

        return f"{timestamp_str:>12}"

    def format_run_id(self, run_id_short: str) -> str:
        """Format run ID - always show the full run ID"""
        return f"{run_id_short:>12}"

    def format(self, item: Log | Event) -> str:
        if isinstance(item, Log):
            return self.format_log(item)
        else:
            return self.format_event(item)

    def format_log(self, log: Log) -> str:
        timestamp = self.format_timestamp(log.timestamp)

        # Get run ID - prefer task_run_id if available, otherwise flow_run_id
        run_id = log.task_run_id or log.flow_run_id
        run_id_short = str(run_id)[-12:] if run_id else "............"
        run_id_display = self.format_run_id(run_id_short)

        # Build the prefix without Rich markup to calculate actual character width
        icon = "▪"
        prefix_plain = f"{icon} {timestamp.strip()} {run_id_display.strip()} "

        # Handle multi-line messages by indenting continuation lines
        lines = log.message.split("\n")
        if len(lines) == 1:
            # Single line message
            return f"[dim]▪[/dim] {timestamp} [dim]{run_id_display}[/dim] {log.message}"

        # Multi-line message - first line with full prefix, subsequent lines indented
        first_line = f"[dim]▪[/dim] {timestamp} [dim]{run_id_display}[/dim] {lines[0]}"
        indent = " " * len(prefix_plain)
        continuation_lines = [f"{indent}{line}" for line in lines[1:]]

        return first_line + "\n" + "\n".join(continuation_lines)

    def format_event(self, event: Event) -> str:
        timestamp = self.format_timestamp(event.occurred)

        # Find run ID from event resources - prefer task-run, fallback to flow-run
        run_id = None

        # Check primary resource first
        if event.resource.id.startswith("prefect.task-run."):
            run_id = event.resource.id.split(".", 2)[
                2
            ]  # Extract UUID after "prefect.task-run."
        elif event.resource.id.startswith("prefect.flow-run."):
            run_id = event.resource.id.split(".", 2)[
                2
            ]  # Extract UUID after "prefect.flow-run."

        # If not found, check related resources
        if not run_id:
            for related in event.related:
                if related.id.startswith("prefect.task-run."):
                    run_id = related.id.split(".", 2)[2]
                    break
                elif related.id.startswith("prefect.flow-run."):
                    run_id = related.id.split(".", 2)[2]
                    break

        run_id_short = run_id[-12:] if run_id else "............"
        run_id_display = self.format_run_id(run_id_short)

        state_colors = {
            # FLOW RUN STATES
            # SCHEDULED
            "prefect.flow-run.Scheduled": "yellow",
            "prefect.flow-run.Late": "yellow",
            "prefect.flow-run.Resuming": "yellow",
            "prefect.flow-run.AwaitingRetry": "yellow",
            "prefect.flow-run.AwaitingConcurrencySlot": "yellow",
            "prefect.task-run.Scheduled": "yellow",
            "prefect.task-run.AwaitingRetry": "yellow",
            # PENDING
            "prefect.flow-run.Pending": "bright_black",
            "prefect.flow-run.Paused": "bright_black",
            "prefect.flow-run.Suspended": "bright_black",
            "prefect.task-run.Pending": "bright_black",
            # RUNNING
            "prefect.flow-run.Running": "blue",
            "prefect.flow-run.Retrying": "blue",
            "prefect.task-run.Running": "blue",
            "prefect.task-run.Retrying": "blue",
            # COMPLETED
            "prefect.flow-run.Completed": "green",
            "prefect.flow-run.Cached": "green",
            "prefect.task-run.Completed": "green",
            "prefect.task-run.Cached": "green",
            # CANCELLED
            "prefect.flow-run.Cancelled": "bright_black",
            "prefect.flow-run.Cancelling": "bright_black",
            "prefect.task-run.Cancelled": "bright_black",
            "prefect.task-run.Cancelling": "bright_black",
            # CRASHED
            "prefect.flow-run.Crashed": "orange1",
            # FAILED
            "prefect.flow-run.Failed": "red",
            "prefect.flow-run.TimedOut": "red",
            "prefect.task-run.Failed": "red",
            "prefect.task-run.TimedOut": "red",
        }

        color = state_colors.get(event.event, "bright_magenta")
        name = event.resource.name or event.resource.id
        return (
            f"[{color}]●[/{color}] {timestamp} [dim]{run_id_display}[/dim] "
            f"{event.event} * [bold cyan]{name}[/bold cyan]"
        )
