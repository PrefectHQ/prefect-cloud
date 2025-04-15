from uuid import UUID
from typing import Annotated

import typer
from rich.table import Table
from rich.text import Text

from prefect_cloud import auth
from prefect_cloud.cli.deployments import app
from prefect_cloud.utilities.tui import redacted


@app.command(rich_help_panel="Auth")
async def login(
    key: Annotated[
        str | None,
        typer.Option(
            "--key",
            "-k",
            help="Prefect Cloud API key",
        ),
    ] = None,
    workspace: Annotated[
        str | None,
        typer.Option(
            "--workspace",
            "-w",
            help="Workspace ID or slug",
        ),
    ] = None,
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Suppress output",
        ),
    ] = False,
):
    """
    Log in to Prefect Cloud

    Examples:
        Interactive login:
        $ prefect-cloud login

        Login with API key:
        $ prefect-cloud login --key your-api-key

        Login to specific workspace:
        $ prefect-cloud login --workspace your-workspace
    """
    app.quiet = quiet

    with app.create_progress() as progress:
        progress.add_task("Logging in to Prefect Cloud...")
        try:
            await auth.login(api_key=key, workspace_id_or_slug=workspace)
        except auth.LoginError:
            app.exit_with_error("[bold]✗[/] Unable to complete login to Prefect Cloud")

    app.exit_with_success("[bold]✓[/] Logged in to Prefect Cloud")


@app.command(rich_help_panel="Auth")
def logout(
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Suppress output",
        ),
    ] = False,
):
    """
    Log out of Prefect Cloud
    """
    app.quiet = quiet

    auth.logout()
    app.exit_with_success("[bold]✓[/] Logged out of Prefect Cloud")


@app.command(rich_help_panel="Auth")
async def whoami() -> None:
    """
    Show current user and workspace information

    Displays:
    • User details
    • Current workspace
    • API configuration
    • Available accounts and workspaces
    """
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

    app.print(table)

    app.print("")

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

    app.print(table)
