import asyncio
import functools
import inspect
import json
import traceback
from typing import Any, Callable, NoReturn

import typer
from click import ClickException
from rich.console import Console
from rich.progress import Progress
from rich.theme import Theme

from prefect_cloud.utilities.exception import MissingProfileError


def exit_with_error(
    message: str | Exception, progress: Progress | None = None
) -> NoReturn:
    from prefect_cloud.cli.root import app

    if progress:
        progress.stop()
    app.console.print(message, style="red")
    raise typer.Exit(1)


def exit_with_success(
    message: str | Exception, progress: Progress | None = None
) -> NoReturn:
    from prefect_cloud.cli.root import app

    if progress:
        progress.stop()
    app.console.print(message, style="green")
    raise typer.Exit(0)


def with_cli_exception_handling(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except (typer.Exit, typer.Abort, ClickException):
            raise  # Do not capture click or typer exceptions
        except MissingProfileError as exc:
            exit_with_error(exc)
        except Exception:
            traceback.print_exc()
            exit_with_error("An exception occurred.")

    return wrapper


def process_key_value_pairs(
    pairs: list[str] | None,
    progress: Progress | None = None,
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
        value = value.strip()

        if as_json:
            try:
                result[key] = json.loads(value)
            except json.JSONDecodeError:
                result[key] = value
        else:
            result[key] = value

    if invalid_pairs:
        exit_with_error(f"Invalid key value pairs: {invalid_pairs}", progress)

    return result


class PrefectCloudTyper(typer.Typer):
    """
    Wraps commands created by `Typer` to support async functions and handle errors.
    """

    console: Console

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
            # asyncio.run(). This has the advantage of keeping the function in
            # the main thread, which means signal handling works for e.g. the
            # server and workers. However, it means that async CLI commands can
            # not directly call other async CLI commands (because asyncio.run()
            # can not be called nested). In that (rare) circumstance, refactor
            # the CLI command so its business logic can be invoked separately
            # from its entrypoint.
            func = inspect.unwrap(original_fn)

            if asyncio.iscoroutinefunction(func):
                async_fn = original_fn

                @functools.wraps(original_fn)
                def sync_fn(*args: Any, **kwargs: Any) -> Any:
                    return asyncio.run(async_fn(*args, **kwargs))

                setattr(sync_fn, "aio", async_fn)
                wrapped_fn = sync_fn
            else:
                wrapped_fn = original_fn

            wrapped_fn = with_cli_exception_handling(wrapped_fn)
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
