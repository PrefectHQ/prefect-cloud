import asyncio
import contextvars
import functools
import inspect
import json
import threading
import traceback
from collections.abc import Coroutine
from typing import Any, Callable, NoReturn, TypeVar, Generator, ContextManager
from contextlib import contextmanager

import typer
from click import ClickException
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.theme import Theme


T = TypeVar("T")


def process_key_value_pairs(
    pairs: list[str] | None,
    as_json: bool = False,
) -> dict[str, Any] | dict[str, str]:
    """
    Process a list of KEY=VALUE pairs into a dictionary.

    Args:
        pairs: List of strings in KEY=VALUE format
        progress: Optional Progress object for status updates
        as_json: If True, attempts to parse values as JSON

    Returns:
        Dictionary of processed key-value pairs. If as_json is True, values
        may be any JSON-serializable type. Otherwise all values are strings.
    """
    if not pairs:
        return {}

    invalid_pairs: list[str] = []
    result = {}

    for pair in pairs:
        parts = pair.split("=", 1)
        if len(parts) != 2:
            invalid_pairs.append(pair)
            continue

        key, value = parts
        if not key or not value:
            invalid_pairs.append(pair)
            continue

        key = key.strip()
        value = value.strip().strip("\"'")

        if as_json:
            try:
                result[key] = json.loads(value)
            except json.JSONDecodeError:
                result[key] = value
        else:
            result[key] = value

    if invalid_pairs:
        raise ValueError(f"Invalid key value pairs: {invalid_pairs}")

    return result  # type: ignore


class PrefectCloudTyper(typer.Typer):
    """
    Wraps commands created by `Typer` to support async functions and handle errors.
    """

    console: Console
    quiet: bool = False

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.console = Console(
            highlight=False,
            theme=Theme({"prompt.choices": "bold blue"}),
            color_system="auto",
        )
        self._current_progress: Progress | None = None

    def add_typer(
        self,
        typer_instance: "PrefectCloudTyper",
        *args: Any,
        no_args_is_help: bool = True,
        aliases: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        This will cause help to be default command for all sub apps unless specifically stated otherwise, opposite of before.
        """
        if aliases:
            for alias in aliases:
                super().add_typer(
                    typer_instance,
                    *args,
                    name=alias,
                    no_args_is_help=no_args_is_help,
                    hidden=True,
                    **kwargs,
                )

        return super().add_typer(
            typer_instance, *args, no_args_is_help=no_args_is_help, **kwargs
        )

    def _with_cli_exception_handling(
        self, fn: Callable[..., Any]
    ) -> Callable[..., Any]:
        """Internal wrapper to handle exceptions in CLI commands."""

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return fn(*args, **kwargs)
            except (typer.Exit, typer.Abort, ClickException):
                raise  # Do not capture click or typer exceptions
            except Exception:
                traceback.print_exc()
                self.exit_with_error("An error occurred.")

        return wrapper

    def command(
        self,
        name: str | None = None,
        *args: Any,
        aliases: list[str] | None = None,
        **kwargs: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Create a new command. If aliases are provided, the same command function
        will be registered with multiple names.
        """

        def wrapper(original_fn: Callable[..., Any]) -> Callable[..., Any]:
            # click doesn't support async functions, so we wrap them in
            # run_sync(). This allows the function to be run in a synchronous
            # context, even from within an async or nested sync frame, providing
            # more flexibility than asyncio.run(). However, note that run_sync
            # is also fallible and should be used with caution in complex
            # scenarios. In rare cases where async CLI commands need to call
            # other async CLI commands, you may need to refactor the CLI command
            # so its business logic can be invoked separately from its
            # entrypoint.
            func = inspect.unwrap(original_fn)

            if asyncio.iscoroutinefunction(func):
                async_fn = original_fn

                @functools.wraps(original_fn)
                def sync_fn(*args: Any, **kwargs: Any) -> Any:
                    return run_sync(async_fn(*args, **kwargs))

                setattr(sync_fn, "aio", async_fn)
                wrapped_fn = sync_fn
            else:
                wrapped_fn = original_fn

            wrapped_fn = self._with_cli_exception_handling(wrapped_fn)
            # register fn with its original name
            command_decorator = super(PrefectCloudTyper, self).command(
                name=name, *args, **kwargs
            )
            original_command = command_decorator(wrapped_fn)

            # register fn for each alias, e.g. @marvin_app.command(aliases=["r"])
            if aliases:
                for alias in aliases:
                    super(PrefectCloudTyper, self).command(
                        name=alias,
                        *args,
                        **{k: v for k, v in kwargs.items() if k != "aliases"},
                    )(wrapped_fn)

            return original_command

        return wrapper

    def setup_console(self, soft_wrap: bool, prompt: bool) -> None:
        self.console = Console(
            highlight=False,
            color_system="auto",
            theme=Theme({"prompt.choices": "bold blue"}),
            soft_wrap=not soft_wrap,
            force_interactive=prompt,
        )

    def create_progress(self, *columns: Any, **kwargs: Any) -> ContextManager[Progress]:
        """Create a progress indicator that respects quiet mode and tracks in context."""
        if not columns:
            columns = (SpinnerColumn(), TextColumn("[dim]{task.description}"))

        @contextmanager
        def progress_maker() -> Generator[Progress, None, None]:
            old_progress = self._current_progress

            prog = Progress(*columns, transient=True, disable=self.quiet, **kwargs)
            self._current_progress = prog

            try:
                with prog:
                    yield prog
            finally:
                self._current_progress = old_progress

        return progress_maker()

    def get_current_progress(self) -> Progress | None:
        """Get the currently active progress bar if any."""
        return self._current_progress

    @contextmanager
    def suppress_progress(self) -> Generator[None, None, None]:
        """
        Context manager that temporarily suppresses the current progress bar.
        Restores the original progress bar after exiting the context.

        Example:
            with app.suppress_progress():
                # code that shouldn't display progress
        """
        if self._current_progress:
            self._current_progress.stop()

        try:
            yield
        finally:
            if self._current_progress:
                self._current_progress.start()

    def print(self, *args: Any, **kwargs: Any) -> None:
        """Print a message unless quiet mode is enabled."""
        if not self.quiet:
            self.console.print(*args, **kwargs)

    def error(self, message: str | Exception, progress: Progress | None = None) -> None:
        """Print an error message unless quiet mode is enabled and exit."""
        progress = progress or self._current_progress
        if progress:
            progress.stop()
        if not self.quiet:
            self.console.print(message, style="red")

    def exit_with_error(self, message: str) -> NoReturn:
        """Print an error message and exit with error code."""
        self.error(message)
        raise typer.Exit(1)

    def success(self, message: str, progress: Progress | None = None) -> None:
        """Print a success message unless quiet mode is enabled."""
        progress = progress or self._current_progress
        if progress:
            progress.stop()
        if not self.quiet:
            self.console.print(message, style="green")

    def exit_with_success(self, message: str) -> NoReturn:
        """Print a success message and exit with success code."""
        self.success(message)
        raise typer.Exit(0)


def run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine synchronously.

    This function uses asyncio to run a coroutine in a synchronous context.
    It attempts the following strategies in order:
    1. If no event loop is running, creates a new one and runs the coroutine
    2. If a loop is running, attempts to run the coroutine on that loop
    3. As a last resort, creates a new thread with its own event loop to run the coroutine

    Context variables are properly propagated between threads in all cases.

    Example:
    ```python
    async def f(x: int) -> int:
        return x + 1

    result = run_sync(f(1))
    ```

    Args:
        coro: The coroutine to run synchronously

    Returns:
        The result of the coroutine
    """
    ctx = contextvars.copy_context()
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    try:
        return ctx.run(loop.run_until_complete, coro)
    except RuntimeError as e:
        if "event loop" in str(e):
            return run_sync_in_thread(coro)
        raise e


def run_sync_in_thread(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine synchronously in a new thread.

    This function creates a new thread with its own event loop to run the coroutine.
    Context variables are properly propagated between threads.
    This is useful when you need to run async code in a context where you can't use
    the current event loop (e.g., inside an async frame).

    Example:
    ```python
    async def f(x: int) -> int:
        return x + 1

    result = run_sync_in_thread(f(1))
    ```

    Args:
        coro: The coroutine to run synchronously

    Returns:
        The result of the coroutine
    """
    result: T | None = None
    error: BaseException | None = None
    done = threading.Event()
    ctx = contextvars.copy_context()

    def thread_target() -> None:
        nonlocal result, error
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            result = ctx.run(loop.run_until_complete, coro)
        except BaseException as e:
            error = e
        finally:
            loop.close()
            asyncio.set_event_loop(None)
            done.set()

    thread = threading.Thread(target=thread_target)
    thread.start()
    done.wait()

    if error is not None:
        raise error

    return result  # type: ignore
