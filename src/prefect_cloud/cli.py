from uuid import UUID

import typer
from prefect.cli._utilities import exit_with_error as _exit_with_error
from prefect.cli.root import PrefectTyper
from prefect.client.base import ServerType, determine_server_type
from prefect.utilities.urls import url_for
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text

from prefect_cloud import auth
from prefect_cloud.client import (
    get_cloud_api_url,
    get_prefect_cloud_client,
)
from prefect_cloud.dependencies import get_dependencies
from prefect_cloud.github import (
    FileNotFound,
    GitHubFileRef,
    get_github_raw_content,
    to_pull_step,
)
from prefect_cloud.utilities.flows import get_parameter_schema_from_content
from prefect_cloud.utilities.tui import redacted

app = PrefectTyper()


def exit_with_error(message: str, progress: Progress = None):
    if progress:
        progress.stop()
    _exit_with_error(message)


def ensure_prefect_cloud():
    if determine_server_type() != ServerType.CLOUD:
        exit_with_error("Not logged into Prefect Cloud! Run `uvx prefect cloud login`.")


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
    ensure_prefect_cloud()

    with Progress(
        SpinnerColumn(),
        TextColumn("[blue]{task.description}"),
        transient=True,
    ) as progress:
        try:
            env_vars = process_key_value_pairs(env) if env else {}
        except ValueError as e:
            exit_with_error(str(e), progress=progress)

        async with get_prefect_cloud_client() as client:
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

            deployment_id = await client.create_managed_deployment(
                deployment_name,
                github_ref.filepath,
                function,
                work_pool,
                pull_steps,
                parameter_schema,
                job_variables={
                    "pip_packages": get_dependencies(dependencies or []),
                    "env": {"PREFECT_CLOUD_API_URL": get_cloud_api_url()} | env_vars,
                },
            )

    app.console.print(
        f"View deployment here: "
        f"\n ➜ [link={url_for('deployment', deployment_id)}]"
        f"{deployment_name}"
        f"[/link]",
        style="blue",
    )

    app.console.print(
        f"Run it with: \n $ prefect deployment run {function}/{deployment_name}",
        style="blue",
    )


@app.command()
async def schedule():
    raise NotImplementedError


@app.command()
async def init():
    raise NotImplementedError


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
def who_am_i():
    ui_url, api_url, api_key = auth.get_cloud_urls_or_login()

    me = auth.me(api_key)
    accounts = auth.get_accounts(api_key)
    workspaces = auth.get_workspaces(api_key)

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
