import asyncio
import functools
import inspect
import traceback
from typing import Any, Callable, List, Optional
from uuid import UUID

import typer
import tzlocal
from click import ClickException
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

from prefect_cloud import auth, completions, deployments
from prefect_cloud.client import get_prefect_cloud_client
from prefect_cloud.dependencies import get_dependencies
from prefect_cloud.github import (
    FileNotFound,
    GitHubFileRef,
    get_github_raw_content,
    to_pull_step,
)
from prefect_cloud.schemas.objects import (
    CronSchedule,
    DeploymentSchedule,
    IntervalSchedule,
    RRuleSchedule,
)
from prefect_cloud.utilities.exception import MissingProfileError
from prefect_cloud.utilities.flows import get_parameter_schema_from_content
from prefect_cloud.utilities.tui import redacted


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
        aliases: Optional[list[str]] = None,
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
        name: Optional[str] = None,
        *args: Any,
        aliases: Optional[List[str]] = None,
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


app = PrefectCloudTyper()


def exit_with_error(message: str | Exception, progress: Progress = None):
    if progress:
        progress.stop()
    app.console.print(message, style="red")
    raise typer.Exit(1)


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


def process_key_value_pairs(env: list[str]) -> dict[str, str]:
    invalid_pairs = []

    for e in env:
        if "=" not in e:
            invalid_pairs.append(e)

    if invalid_pairs:
        raise ValueError(f"Invalid key value pairs: {invalid_pairs}")

    return {k: v for k, v in [e.split("=") for e in env]}


@app.command()
async def deploy(
    function: str,
    file: str = typer.Option(
        ...,
        "--from",
        "-f",
        help=".py file containing the function to deploy.",
    ),
    dependencies: list[str] = typer.Option(
        ...,
        "--with",
        "-d",
        help="Dependencies to include. Can be a single package `--with prefect`, "
        "multiple packages `--with prefect --with pandas`, "
        "the path to a requirements or pyproject.toml file "
        "`--with requirements.txt / pyproject.toml`.",
        default_factory=list,
    ),
    env: list[str] = typer.Option(
        ...,
        "--env",
        "-e",
        help="Environment variables to set in the format KEY=VALUE. Can be specified multiple times.",
        default_factory=list,
    ),
    credentials: str | None = typer.Option(
        None,
        "--credentials",
        "-c",
        help="Optional credentials if code is in a private repository. ",
    ),
):
    with Progress(
        SpinnerColumn(),
        TextColumn("[blue]{task.description}"),
        transient=True,
    ) as progress:
        try:
            env_vars = process_key_value_pairs(env) if env else {}
        except ValueError as e:
            exit_with_error(str(e), progress=progress)

        async with await get_prefect_cloud_client() as client:
            task = progress.add_task("Inspecting code...", total=None)

            github_ref = GitHubFileRef.from_url(file)
            try:
                raw_contents = await get_github_raw_content(github_ref, credentials)
            except FileNotFound:
                exit_with_error(
                    "Can't access that file in Github. It either doesn't exist or is private. "
                    "If it's private repo retry with `--credentials`.",
                    progress=progress,
                )
            try:
                parameter_schema = get_parameter_schema_from_content(
                    raw_contents, function
                )
            except ValueError:
                exit_with_error(
                    f"Could not find function '{function}' in {github_ref.filepath}",
                    progress=progress,
                )

            progress.update(task, description="Confirming work pool exists...")
            work_pool = await client.ensure_managed_work_pool()

            progress.update(task, description="Deploying flow...")
            deployment_name = f"{function}_deployment"

            credentials_name = None
            if credentials:
                progress.update(task, description="Syncing credentials...")
                credentials_name = f"{github_ref.owner}-{github_ref.repo}-credentials"
                await client.create_credentials_secret(credentials_name, credentials)

            pull_steps = [
                to_pull_step(github_ref, credentials_name)
                # TODO: put back flowify if this a public repo? need to figure that out.
            ]

            _, api_url, _ = await auth.get_cloud_urls_or_login()

            # TODO temporary: remove this when the PR is merged
            pip_packages = [
                "git+https://github.com//PrefectHQ/prefect.git@add-missing-convert-statement"
            ]
            if dependencies:
                pip_packages += get_dependencies(dependencies)

            deployment_id = await client.create_managed_deployment(
                deployment_name,
                github_ref.filepath,
                function,
                work_pool,
                pull_steps,
                parameter_schema,
                job_variables={
                    "pip_packages": pip_packages,
                    "env": {"PREFECT_CLOUD_API_URL": api_url} | env_vars,
                },
            )
    deployment_url = f"{api_url}/deployments/{deployment_id}"
    app.console.print(
        f"View deployment here: \n ➜ [link={deployment_url}]{deployment_name}[/link]",
    )

    app.console.print(
        f"Run it with: \n $ prefect-cloud run {function}/{deployment_name}",
    )


@app.command()
async def run(
    deployment: str = typer.Argument(
        ...,
        help="The deployment to run (either its name or ID).",
        autocompletion=completions.complete_deployment,
    ),
):
    ui_url, _, _ = await auth.get_cloud_urls_or_login()
    flow_run = await deployments.run(deployment)
    flow_run_url = f"{ui_url}/runs/flow-run/{flow_run.id}"
    app.console.print(
        f"Flow run [bold]{flow_run.name}[/bold] [dim]({flow_run.id})[/dim] created",
        f"and will begin running soon.\n"
        f"[link={flow_run_url}]View its progress on Prefect Cloud[/link].",
    )


@app.command()
async def ls():
    context = await deployments.list()

    table = Table(title="Deployments")
    table.add_column("Name")
    table.add_column("Schedule")
    table.add_column("Next run")
    table.add_column("ID")

    def describe_schedule(schedule: DeploymentSchedule) -> Text:
        prefix = "✓" if schedule.active else " "
        style = "dim" if not schedule.active else "green"

        if isinstance(schedule.schedule, CronSchedule):
            description = f"{schedule.schedule.cron} ({schedule.schedule.timezone})"
        elif isinstance(schedule.schedule, IntervalSchedule):
            description = f"Every {schedule.schedule.interval} seconds"
        elif isinstance(schedule.schedule, RRuleSchedule):
            description = f"{schedule.schedule.rrule}"
        else:
            return "TODO"

        return Text(f"{prefix} {description})", style=style)

    for deployment in context.deployments:
        scheduling = Text("\n").join(
            describe_schedule(schedule) for schedule in deployment.schedules
        )

        next_run = context.next_runs_by_deployment_id.get(deployment.id)
        if next_run:
            next_run_time = next_run.expected_start_time.astimezone(
                tzlocal.get_localzone()
            ).strftime("%Y-%m-%d %H:%M:%S %Z")
        else:
            next_run_time = ""

        table.add_row(
            f"{context.flows_by_id[deployment.flow_id].name}/{deployment.name}",
            scheduling,
            next_run_time,
            str(deployment.id),
        )

    app.console.print(table)

    app.console.print(
        "* Cron cheatsheet: minute hour day-of-month month day-of-week",
        style="dim",
    )


@app.command()
async def schedule(
    deployment: str = typer.Argument(
        ...,
        help="The deployment to schedule (either its name or ID).",
        autocompletion=completions.complete_deployment,
    ),
    schedule: str = typer.Argument(
        ...,
        help="The schedule to set, as a cron string. Use 'none' to unschedule.",
    ),
):
    await deployments.schedule(deployment, schedule)


@app.command()
async def pause(
    deployment: str = typer.Argument(
        ...,
        help="The deployment to pause (either its name or ID).",
        autocompletion=completions.complete_deployment,
    ),
):
    await deployments.pause(deployment)


@app.command()
async def resume(
    deployment: str = typer.Argument(
        ...,
        help="The deployment to resume (either its name or ID).",
        autocompletion=completions.complete_deployment,
    ),
):
    await deployments.resume(deployment)


@app.command()
async def login(
    key: str = typer.Option(None, "--key", "-k"),
    workspace: str = typer.Option(None, "--workspace", "-w"),
):
    await auth.login(api_key=key, workspace_id_or_slug=workspace)


@app.command()
def logout():
    auth.logout()


@app.command(aliases=["whoami", "me"])
async def who_am_i() -> None:
    ui_url, api_url, api_key = await auth.get_cloud_urls_or_login()

    me = await auth.me(api_key)
    accounts = await auth.get_accounts(api_key)
    workspaces = await auth.get_workspaces(api_key)

    table = Table(title="User", show_header=False)
    table.add_column("Property")
    table.add_column("Value")

    table.add_row("Name", f"{me.first_name} {me.last_name}")
    table.add_row("Email", me.email)
    table.add_row("Handle", me.handle)
    table.add_row("ID", str(me.id))
    table.add_row("Dashboard", ui_url)
    table.add_row("API URL", api_url)
    table.add_row("API Key", redacted(api_key))

    app.console.print(table)

    app.console.print("")

    table = Table(title="Accounts and Workspaces", show_header=True)
    table.add_column("Account")
    table.add_column("Handle")
    table.add_column("ID")

    workspaces_by_account: dict[UUID, list[auth.Workspace]] = {}
    for workspace in workspaces:
        if workspace.account_id not in workspaces_by_account:
            workspaces_by_account[workspace.account_id] = []
        workspaces_by_account[workspace.account_id].append(workspace)

    for account in accounts:
        if account != accounts[0]:
            table.add_row("", "", "")

        table.add_row(
            Text(account.account_name, style="bold"),
            Text(account.account_handle, style="bold"),
            Text(str(account.account_id), style="bold"),
        )

        account_workspaces = workspaces_by_account.get(account.account_id, [])
        for i, workspace in enumerate(account_workspaces):
            table.add_row(
                Text(
                    account.account_handle
                    if i == 0 and account.account_handle != account.account_name
                    else "",
                    style="dim italic",
                ),
                Text(workspace.workspace_handle),
                Text(str(workspace.workspace_id)),
            )

    app.console.print(table)
