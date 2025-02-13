from uuid import UUID

import typer
import tzlocal
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text

from prefect_cloud import auth, deployments
from prefect_cloud.cli import completions
from prefect_cloud.cli.utilities import (
    PrefectCloudTyper,
    exit_with_error,
    process_key_value_pairs,
)
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
from prefect_cloud.utilities.flows import get_parameter_schema_from_content
from prefect_cloud.utilities.tui import redacted
from prefect_cloud.utilities.blocks import safe_block_name

app = PrefectCloudTyper(
    rich_markup_mode=True, help="⚡ Deploy code to Prefect Cloud ⚡"
)


@app.command(rich_help_panel="Deploy")
async def deploy(
    function: str = typer.Argument(
        help="The python function to deploy.", rich_help_panel="Function"
    ),
    file: str = typer.Option(
        ...,
        "--from",
        "-f",
        help=(
            "URL to a .py file containing the function to deploy."
            "\n\nSupported formats: "
            "\n\n - Github: [https://]github.com/owner/repo/(blob|tree)/ref/path/to/file"
        ),
        rich_help_panel="Source",
        show_default=False,
    ),
    credentials: str | None = typer.Option(
        None,
        "--credentials",
        "-c",
        help="Optional credentials if code is in a private repository. ",
        rich_help_panel="Source",
        show_default=False,
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
        rich_help_panel="Configuration",
        show_default=False,
    ),
    env: list[str] = typer.Option(
        ...,
        "--env",
        "-e",
        help="Environment variables to set in the format KEY=VALUE. Can be specified multiple times.",
        default_factory=list,
        rich_help_panel="Configuration",
        show_default=False,
    ),
    run: bool = typer.Option(
        False,
        "--run",
        "-r",
        help="Run immediately after deploying.",
    ),
    parameters: list[str] = typer.Option(
        ...,
        "--parameters",
        "-p",
        help="Parameters to set in the format NAME=VALUE. Can be specified multiple times. Only used with --run.",
        default_factory=list,
        rich_help_panel="Run Options",
        show_default=False,
    ),
):
    """
    Deploy a function to Prefect Cloud
    """
    ui_url, api_url, _ = await auth.get_cloud_urls_or_login()

    async with await auth.get_prefect_cloud_client() as client:
        with Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task("Inspecting code...", total=None)

            # Process env vars
            try:
                env_vars = process_key_value_pairs(env) if env else {}
            except ValueError as e:
                exit_with_error(str(e), progress=progress)

            # Process parameters
            try:
                func_kwargs = process_key_value_pairs(parameters) if parameters else {}
            except ValueError as e:
                exit_with_error(str(e), progress=progress)

            # Get code contents
            github_ref = GitHubFileRef.from_url(file)
            raw_contents: str | None = None
            try:
                raw_contents = await get_github_raw_content(github_ref, credentials)
            except FileNotFound:
                exit_with_error(
                    "Unable to access file in Github. "
                    "If it's in a private repository retry with `--credentials`.",
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

            progress.update(task, description="Provisioning infrastructure...")
            work_pool = await client.ensure_managed_work_pool()

            deployment_name = f"{function}"

            credentials_name = None
            if credentials:
                progress.update(task, description="Syncing credentials...")
                credentials_name = safe_block_name(
                    f"{github_ref.owner}-{github_ref.repo}-credentials"
                )
                await client.create_credentials_secret(credentials_name, credentials)

            pull_steps = [to_pull_step(github_ref, credentials_name)]

            # TODO temporary: remove this when the PR is merged
            pip_packages = [
                "git+https://github.com//PrefectHQ/prefect.git@add-missing-convert-statement"
            ]
            if dependencies:
                pip_packages += get_dependencies(dependencies)

            progress.update(task, description="Deploying code...")

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

            progress.update(task, completed=True, description="Code deployed!")

        deployment_url = f"{ui_url}/deployments/deployment/{deployment_id}"

        app.console.print(
            f"[bold]Deployed [cyan]{deployment_name}[/cyan]! 🎉[/bold]",
            "\n└─►",
            Text(deployment_url, style="link", justify="left"),
            soft_wrap=True,
        )

        if run:
            flow_run = await client.create_flow_run_from_deployment_id(
                deployment_id, func_kwargs
            )
            flow_run_url = f"{ui_url}/runs/flow-run/{flow_run.id}"
            app.console.print(
                f"[bold]Started flow run [cyan]{flow_run.name}[/cyan]! 🚀[/bold]\n└─►",
                Text(flow_run_url, style="link", justify="left"),
                soft_wrap=True,
            )
        else:
            app.console.print(
                "[bold]Run it with:[/bold]"
                f"\n└─► [green]prefect-cloud run {function}/{deployment_name}[/green]"
            )


@app.command(rich_help_panel="Deploy")
async def run(
    deployment: str = typer.Argument(
        ...,
        help="The deployment to run (either its name or ID).",
        autocompletion=completions.complete_deployment,
    ),
):
    """
    Run a deployment
    """
    ui_url, _, _ = await auth.get_cloud_urls_or_login()
    flow_run = await deployments.run(deployment)
    flow_run_url = f"{ui_url}/runs/flow-run/{flow_run.id}"

    app.console.print(
        f"[bold]Started flow run [cyan]{flow_run.name}[/cyan]! 🚀[/bold]\n└─►",
        Text(flow_run_url, style="link", justify="left"),
        soft_wrap=True,
    )


@app.command(rich_help_panel="Deploy")
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
    """
    Schedule a deployment
    """
    await deployments.schedule(deployment, schedule)


@app.command(rich_help_panel="Manage Deployments")
async def ls():
    """
    List deployments
    """
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
        elif isinstance(schedule.schedule, RRuleSchedule):  # type: ignore[reportUnnecessaryIsInstance]
            description = f"{schedule.schedule.rrule}"
        else:
            app.console.print(f"Unknown schedule type: {type(schedule.schedule)}")
            description = "Unknown"

        return Text(f"{prefix} {description})", style=style)

    for deployment in context.deployments:
        scheduling = Text("\n").join(
            describe_schedule(schedule) for schedule in deployment.schedules
        )

        next_run = context.next_runs_by_deployment_id.get(deployment.id)
        if next_run and next_run.expected_start_time:
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


@app.command(rich_help_panel="Manage Deployments")
async def pause(
    deployment: str = typer.Argument(
        ...,
        help="The deployment to pause (either its name or ID).",
        autocompletion=completions.complete_deployment,
    ),
):
    """
    Pause a scheduled deployment
    """
    await deployments.pause(deployment)


@app.command(rich_help_panel="Manage Deployments")
async def resume(
    deployment: str = typer.Argument(
        ...,
        help="The deployment to resume (either its name or ID).",
        autocompletion=completions.complete_deployment,
    ),
):
    """
    Resume a scheduled deployment
    """
    await deployments.resume(deployment)


@app.command(rich_help_panel="Auth")
async def login(
    key: str = typer.Option(None, "--key", "-k"),
    workspace: str = typer.Option(None, "--workspace", "-w"),
):
    """
    Log in to Prefect Cloud
    """
    await auth.login(api_key=key, workspace_id_or_slug=workspace)


@app.command(rich_help_panel="Auth")
def logout():
    """
    Log out of Prefect Cloud
    """
    auth.logout()


@app.command(rich_help_panel="Auth")
async def whoami() -> None:
    """
    Display logged-in user information
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
