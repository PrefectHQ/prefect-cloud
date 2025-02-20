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
    GitHubRepo,
)
from prefect_cloud.schemas.objects import (
    CronSchedule,
    DeploymentSchedule,
    IntervalSchedule,
    RRuleSchedule,
)
from prefect_cloud.utilities.blocks import safe_block_name
from prefect_cloud.utilities.callables import get_parameter_schema_from_content
from prefect_cloud.utilities.tui import redacted

app = PrefectCloudTyper(
    rich_markup_mode=True,
    help="Deploy with Prefect Cloud",
    short_help="Deploy with Prefect Cloud",
)


@app.command(rich_help_panel="Deploy")
async def deploy(
    function: str = typer.Argument(
        help="The path to the Python function to deploy in <path/to/file.py:function_name> format",
        show_default=False,
    ),
    repo: str = typer.Option(
        ...,
        "--from",
        "-f",
        help=(
            "GitHub repository URL. e.g.\n\n"
            "• Repo: github.com/owner/repo\n\n"
            "• Specific branch: github.com/owner/repo/tree/<branch>\n\n"
            "• Specific commit: github.com/owner/repo/tree/<commit-sha>\n\n"
        ),
        rich_help_panel="Source",
        show_default=False,
    ),
    credentials: str | None = typer.Option(
        None,
        "--credentials",
        "-c",
        help="GitHub credentials for accessing private repositories",
        rich_help_panel="Source",
        show_default=False,
    ),
    dependencies: list[str] = typer.Option(
        ...,
        "--with",
        "-d",
        help=("Python dependencies to include (can be used multiple times)"),
        default_factory=list,
        rich_help_panel="Dependencies",
        show_default=False,
    ),
    with_requirements: str | None = typer.Option(
        None,
        "--with-requirements",
        help="Path to repository's requirements file",
        rich_help_panel="Dependencies",
        show_default=False,
    ),
    env: list[str] = typer.Option(
        ...,
        "--env",
        "-e",
        help="Environment variables in <KEY=VALUE> format (can be used multiple times)",
        default_factory=list,
        rich_help_panel="Configuration",
        show_default=False,
    ),
):
    """
    Deploy a Python function to Prefect Cloud

    Examples:

    Deploy a function:
    $ prefect-cloud deploy flows/hello.py:my_function --from github.com/owner/repo

    Deploy with a requirements file:
    $ prefect-cloud deploy flows/hello.py:my_function --from github.com/owner/repo --with-requirements requirements.txt
    """
    ui_url, api_url, _ = await auth.get_cloud_urls_or_login()

    # Split function_path into file path and function name
    try:
        filepath, function = function.split(":")
        filepath = filepath.lstrip("/")
    except ValueError:
        exit_with_error("Invalid function. Expected path/to/file.py:function_name")

    async with await auth.get_prefect_cloud_client() as client:
        with Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task("Inspecting code...", total=None)

            # Pre-process CLI arguments
            env_vars = process_key_value_pairs(env, progress=progress)

            # Get repository info and file contents
            github_ref = GitHubRepo.from_url(repo)
            try:
                raw_contents = await github_ref.get_file_contents(filepath, credentials)
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
                    f"Could not find function '{function}' in {filepath}",
                    progress=progress,
                )

            progress.update(task, description="Provisioning infrastructure...")
            work_pool = await client.ensure_managed_work_pool()

            credentials_name = None
            if credentials:
                progress.update(task, description="Syncing credentials...")
                credentials_name = safe_block_name(
                    f"{github_ref.owner}-{github_ref.repo}-credentials"
                )
                await client.create_credentials_secret(credentials_name, credentials)

            progress.update(task, description="Deploying code...")

            pull_steps = [github_ref.to_pull_step(credentials_name)]
            if dependencies:
                quoted_dependencies = [
                    f"'{dependency}'" for dependency in get_dependencies(dependencies)
                ]
                pull_steps.append(
                    {
                        "prefect.deployments.steps.run_shell_script": {
                            "directory": "{{ git-clone.directory }}",
                            "script": f"uv pip install {' '.join(quoted_dependencies)}",
                        }
                    }
                )
            if with_requirements:
                pull_steps.append(
                    {
                        "prefect.deployments.steps.run_shell_script": {
                            "directory": "{{ git-clone.directory }}",
                            "script": f"uv pip install -r {with_requirements}",
                        }
                    }
                )

            deployment_name = f"{function}"
            deployment_id = await client.create_managed_deployment(
                deployment_name=deployment_name,
                filepath=filepath,
                function=function,
                work_pool_name=work_pool.name,
                pull_steps=pull_steps,
                parameter_schema=parameter_schema,
                job_variables={
                    "env": {"PREFECT_CLOUD_API_URL": api_url} | env_vars,
                },
            )

            progress.update(task, completed=True, description="Code deployed!")

        deployment_url = f"{ui_url}/deployments/deployment/{deployment_id}"
        run_prefix = "├─► Run: "
        run_cmd = f"prefect-cloud run {function}/{deployment_name}"
        schedule_prefix = "├─► Schedule: "
        schedule_cmd = f"prefect-cloud schedule {function}/{deployment_name} <SCHEDULE>"
        blank_space_len = (
            len(schedule_prefix + schedule_cmd) - len(run_prefix + run_cmd) + 1
        )
        deployed_msg = f"[bold]Deployed [cyan]{deployment_name}[/cyan]! 🎉[/bold]"
        left_border = " ┌── "
        right_border = " ─"

        # Use schedule length directly since it's always longer
        remaining_dashes = (
            len(schedule_prefix + schedule_cmd)
            - (len(left_border) + len(right_border))
            - len(f"Deployed {deployment_name}! 🎉")
            + 1
        )

        app.console.print(
            f"{left_border}{deployed_msg}{right_border}{'─' * remaining_dashes}┐\n",
            f"{run_prefix}[bold][cyan]{run_cmd}[/cyan][/bold]{' ' * blank_space_len}│\n",
            f"{schedule_prefix}[bold][cyan]{schedule_cmd}[/cyan][/bold] ┘ \n",
            "└─► View:",
            Text(deployment_url, style="link", justify="left"),
            soft_wrap=True,
        )

        if work_pool.is_paused:
            work_pool_url = f"{ui_url}/work-pools"
            app.console.print(
                "[bold][orange1]Note:[/orange1][/bold] Your managed work pool is",
                "currently [bold]paused[/bold]. This will prevent the deployment "
                "from running until it is [bold]resumed[/bold].  Visit",
                Text(work_pool_url, style="link", justify="left"),
                "to resume the work pool.",
                soft_wrap=True,
            )


@app.command(rich_help_panel="Deploy")
async def run(
    deployment: str = typer.Argument(
        ...,
        help="Name or ID of the deployment to run",
        autocompletion=completions.complete_deployment,
    ),
    parameters: list[str] = typer.Option(
        ...,
        "--parameter",
        "-p",
        help="Function parameter in <NAME=VALUE> format (can be used multiple times)",
        default_factory=list,
        rich_help_panel="Run",
        show_default=False,
    ),
):
    """
    Run a deployment immediately

    Examples:
        $ prefect-cloud run flow_name/deployment_name
    """
    ui_url, _, _ = await auth.get_cloud_urls_or_login()
    func_kwargs = process_key_value_pairs(parameters, as_json=True)

    flow_run = await deployments.run(deployment, func_kwargs)
    flow_run_url = f"{ui_url}/runs/flow-run/{flow_run.id}"

    app.console.print(
        f"[bold]Started flow run [cyan]{flow_run.name}[/cyan]! 🚀[/bold]\n└─► View:",
        Text(flow_run_url, style="link", justify="left"),
        soft_wrap=True,
    )

    async with await auth.get_prefect_cloud_client() as client:
        deployment_ = await deployments.get_deployment(deployment)
        work_pool = await client.read_work_pool_by_name(deployment_.work_pool_name)

    work_pool_url = f"{ui_url}/work-pools"
    if work_pool.is_paused:
        app.console.print(
            "\n",
            "[bold][orange1]Note:[/orange1][/bold] Your managed work pool is",
            "currently [bold]paused[/bold]. This will prevent the deployment "
            "from running until it is [bold]resumed[/bold].  Visit",
            Text(work_pool_url, style="link", justify="left"),
            "to resume the work pool.",
            soft_wrap=True,
        )


@app.command(rich_help_panel="Deploy")
async def schedule(
    deployment: str = typer.Argument(
        ...,
        help="Name or ID of the deployment to schedule",
        autocompletion=completions.complete_deployment,
    ),
    schedule: str = typer.Argument(
        ...,
        help="Cron schedule string or 'none' to unschedule",
    ),
    parameters: list[str] = typer.Option(
        ...,
        "--parameter",
        "-p",
        help="Function parameter in <NAME=VALUE> format (can be used multiple times)",
        default_factory=list,
    ),
):
    """
    Set a deployment to run on a schedule

    Examples:
        Run daily at midnight:
        $ prefect-cloud schedule flow_name/deployment_name "0 0 * * *"

        Run every hour:
        $ prefect-cloud schedule flow_name/deployment_name "0 * * * *"

        Remove schedule:
        $ prefect-cloud schedule flow_name/deployment_name none
    """
    func_kwargs = process_key_value_pairs(parameters, as_json=True)
    await deployments.schedule(deployment, schedule, func_kwargs)


@app.command(rich_help_panel="Deploy")
async def unschedule(
    deployment: str = typer.Argument(
        ...,
        help="Name or ID of the deployment to remove schedules from",
    ),
):
    """
    Remove deployment schedules
    """
    await deployments.schedule(deployment, "none")


@app.command(rich_help_panel="Deploy")
async def ls():
    """
    List all deployments
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


@app.command(rich_help_panel="Deploy")
async def delete(
    deployment: str = typer.Argument(
        ...,
        help="Name or ID of the deployment to delete",
        autocompletion=completions.complete_deployment,
    ),
):
    """
    Delete a deployment
    """
    await deployments.delete(deployment)


@app.command(rich_help_panel="Auth")
async def login(
    key: str = typer.Option(None, "--key", "-k", help="Prefect Cloud API key"),
    workspace: str = typer.Option(
        None, "--workspace", "-w", help="Workspace ID or slug"
    ),
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
