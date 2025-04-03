from uuid import UUID
from typing import Annotated, Any

import typer
import tzlocal
import rich
from rich.console import Group
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich.rule import Rule
from rich.padding import Padding

from prefect_cloud import auth, deployments
from prefect_cloud.py_versions import PythonVersion
from prefect_cloud.cli import completions
from prefect_cloud.cli.utilities import (
    PrefectCloudTyper,
    process_key_value_pairs,
)
from prefect_cloud.dependencies import get_dependencies
from prefect_cloud.github import (
    FileNotFound,
    GitHubRepo,
    infer_repo_url,
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
    function: Annotated[
        str,
        typer.Argument(
            help="The path to the Python function to deploy in <path/to/file.py:function_name> format",
            show_default=False,
        ),
    ],
    repo: Annotated[
        str,
        typer.Option(
            "--from",
            "-f",
            default_factory=infer_repo_url,
            autocompletion=completions.complete_repo,
            help=(
                "GitHub repository reference in any of these formats:\n\n"
                "• owner/repo\n\n"
                "• owner/repo/tree/branch\n\n"
                "• owner/repo/tree/commit-sha\n\n"
                "If not provided, the repository of the current directory will be used."
            ),
            rich_help_panel="Source",
            show_default=False,
        ),
    ],
    credentials: Annotated[
        str | None,
        typer.Option(
            "--credentials",
            "-c",
            help="GitHub credentials for accessing private repositories",
            rich_help_panel="Source",
            show_default=False,
        ),
    ] = None,
    dependencies: Annotated[
        list[str] | None,
        typer.Option(
            "--with",
            "-d",
            help=("Python dependencies to include (can be used multiple times)"),
            rich_help_panel="Environment",
            show_default=False,
        ),
    ] = None,
    with_requirements: Annotated[
        str | None,
        typer.Option(
            "--with-requirements",
            help="Path to repository's requirements file",
            rich_help_panel="Environment",
            show_default=False,
        ),
    ] = None,
    with_python: Annotated[
        PythonVersion,
        typer.Option(
            "--with-python",
            help="Python version to use at runtime",
            rich_help_panel="Environment",
            case_sensitive=False,
        ),
    ] = PythonVersion.PY_312,
    env: Annotated[
        list[str] | None,
        typer.Option(
            "--env",
            "-e",
            help="Environment variables in <KEY=VALUE> format (can be used multiple times)",
            rich_help_panel="Environment",
            show_default=False,
        ),
    ] = None,
    parameters: Annotated[
        list[str] | None,
        typer.Option(
            "--parameter",
            "-p",
            help="Parameter default values in <NAME=VALUE> format (can be used multiple times)",
            rich_help_panel="Environment",
            show_default=False,
        ),
    ] = None,
    deployment_name: Annotated[
        str | None,
        typer.Option(
            "--name",
            "-n",
            help="A name for the deployment. If not provided, the function name will be used.",
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
) -> UUID:
    """
    Deploy a Python function to Prefect Cloud

    Examples:

    Deploy a function:
    $ prefect-cloud deploy flows/hello.py:my_function --from github.com/owner/repo

    Deploy with a requirements file:
    $ prefect-cloud deploy flows/hello.py:my_function --from github.com/owner/repo --with-requirements requirements.txt
    """
    app.quiet = quiet

    # Initialize default values
    dependencies = dependencies or []
    env = env or []
    parameters = parameters or []

    ui_url, api_url, _ = await auth.get_cloud_urls_or_login()

    # Split function_path into file path and function name
    try:
        filepath, function = function.split(":")
        filepath = filepath.lstrip("/")
    except ValueError:
        app.exit_with_error("Invalid function. Expected path/to/file.py:function_name")

    async with await auth.get_prefect_cloud_client() as client:
        with app.create_progress() as progress:
            task = progress.add_task("Connecting to repo...")
            env_vars = process_key_value_pairs(env)
            parameter_defaults = process_key_value_pairs(parameters, as_json=True)
            pull_steps: list[dict[str, Any]] = []
            github_ref = GitHubRepo.from_url(repo)

            try:
                # via `--credentials`
                if credentials:
                    raw_contents = await github_ref.get_file_contents(
                        filepath, credentials
                    )
                    block_name = safe_block_name(
                        f"{github_ref.owner}-{github_ref.repo}-credentials"
                    )
                    await client.create_credentials_secret(
                        name=block_name, credentials=credentials
                    )
                    pull_steps.extend(
                        github_ref.private_repo_via_block_pull_steps(block_name)
                    )
                    auth_method = "Provided Credentials / PAT"
                # via GitHub App installation
                elif credentials_via_app := await client.get_github_token(
                    github_ref.owner, github_ref.repo
                ):
                    raw_contents = await github_ref.get_file_contents(
                        filepath, credentials_via_app
                    )
                    pull_steps.extend(
                        github_ref.private_repo_via_github_app_pull_steps()
                    )
                    auth_method = "GitHub App"
                # Otherwise assume public repo
                else:
                    raw_contents = await github_ref.get_file_contents(filepath)
                    pull_steps.extend(github_ref.public_repo_pull_steps())
                    auth_method = "Public Repo"
            except FileNotFound:
                app.exit_with_error(
                    f"Unable to access file [bold]{filepath}[/] in [bold]{github_ref.owner}/{github_ref.repo}[/]. "
                    f"Make sure the file exists and is accessible.\n\n"
                    f"If this is a private repository, you can\n"
                    f"1. [bold](recommended)[/] Install the Prefect Cloud GitHub App with:\n"
                    "prefect-cloud github setup\n"
                    f"2. Pass credentials directly via  --credentials",
                )

            # Process function parameters
            try:
                parameter_schema = get_parameter_schema_from_content(
                    raw_contents, function
                )
            except ValueError:
                app.exit_with_error(
                    f"Could not find function '{function}' in {filepath}",
                )

            # Provision infrastructure
            progress.update(task, description="Provisioning infrastructure...")
            work_pool = await client.ensure_managed_work_pool()

            progress.update(task, description="Deploying...")

            # Create Deployment
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

            deployment_name = deployment_name or f"{function}"
            deployment_id = await client.create_managed_deployment(
                deployment_name=deployment_name,
                filepath=filepath,
                function=function,
                work_pool_name=work_pool.name,
                pull_steps=pull_steps,
                parameter_schema=parameter_schema,
                job_variables={
                    "env": {"PREFECT_CLOUD_API_URL": api_url} | env_vars,
                    "image": PythonVersion.to_prefect_image(with_python),
                },
                parameters=parameter_defaults,
            )
        function_name = function
        final_deployment_name = deployment_name

        # --- Prepare Display Variables ---
        # ... (same variable prep as before) ...
        deployment_url = f"{ui_url}/deployments/deployment/{deployment_id}"
        run_cmd = f"prefect-cloud run {function_name}/{final_deployment_name}"
        schedule_cmd = (
            f"prefect-cloud schedule {function_name}/{final_deployment_name} <SCHEDULE>"
        )
        repo_display = f"{github_ref.owner}/{github_ref.repo}"
        repo_url = getattr(
            github_ref,
            "url",
            f"https://github.com/{github_ref.owner}/{github_ref.repo}",
        )
        if github_ref.ref and github_ref.ref != "main":
            repo_url += f"/tree/{github_ref.ref}"
        ref_text = (
            f"@{github_ref.ref}" if github_ref.ref and github_ref.ref != "main" else ""
        )
        pool_type = getattr(work_pool, "type", "managed")
        link_style = f"link {deployment_url} underline"
        repo_link_style = f"link {repo_url} underline"
        work_pool_url = f"{ui_url}/work-pools"
        work_pool_link_style = f"link {work_pool_url} underline"
        status_text = (
            Text("ACTIVE", style="green")
            if not work_pool.is_paused
            else Text("PAUSED", style="bold red")
        )

        # --- Assemble Content for the Single Panel ---
        panel_content = []

        # 1. Source Section (CLI-Style Title, Left Aligned)
        # Using Padding(..., (1,1,0,1)) adds space ABOVE the rule too
        panel_content.append(
            Padding(
                Rule("  📁 Source ", style="dim", characters="─", align="left"),
                (1, 0, 0, 0),
            )
        )
        panel_content.append(
            Padding(
                Text.assemble(
                    ("  Function:    ", "dim"),
                    (f"{function_name}\n"),
                    ("  Entrypoint:  ", "dim"),
                    (f"{filepath}\n", ""),
                    ("  Repository:  ", "dim"),
                    (f"{repo_display}\n", repo_link_style),
                    (f"{ref_text}", "dim"),
                    ("  Auth:        ", "dim"),
                    (f"{auth_method}"),
                ),
                (0, 1, 0, 1),
            )
        )

        # 2. Compute Section (CLI-Style Title, Left Aligned)
        panel_content.append(
            Padding(
                Rule("  🚀 Compute ", style="dim", characters="─", align="left"),
                (1, 0, 0, 0),
            )
        )
        compute_details = Text.assemble(
            ("  Work Pool:   ", "dim"),
            (f"{work_pool.name}\n", ""),
            ("  Type:        ", "dim"),
            (f"{pool_type}\n"),
            ("  Status:      ", "dim"),
            status_text,
        )
        panel_content.append(Padding(compute_details, (0, 1, 0, 1)))

        env_items = []
        env_items.append(
            Text.assemble(("  Python Version:  ", "dim"), (f"{with_python.value}", ""))
        )
        if with_requirements:
            env_items.append(
                Text.assemble(
                    ("  Dependencies:        ", "dim"),
                    (f"from '{with_requirements}'", ""),
                )
            )
        if dependencies:
            dep_display = ", ".join(dependencies[:3]) + (
                "..." if len(dependencies) > 3 else ""
            )
            env_items.append(
                Text.assemble(
                    ("  Extra Dependencies:        ", "dim"), (dep_display, "")
                )
            )
        if env_vars:
            var_display = ", ".join(env_vars.keys())
            env_items.append(Text.assemble(("  Env:   ", "dim"), (var_display, "dim")))
        if parameter_defaults:
            param_display = ", ".join(parameter_defaults.keys())
            env_items.append(
                Text.assemble(("  Params Set:  ", "dim"), (param_display, "dim"))
            )

        panel_content.append(
            Padding(
                Rule("  📦 Environment ", style="dim", characters="─", align="left"),
                (1, 0, 0, 0),
            )
        )
        padded_items = [Padding(item, (0, 0, 0, 1)) for item in env_items]
        environment_panel_content = Group(*padded_items)
        panel_content.append(environment_panel_content)

        grouped_content = Group(*panel_content)

        app.print(
            Panel(
                renderable=grouped_content,
                title=f"[bold][green][ {final_deployment_name} // DEPLOYED ][/][/bold]",
                title_align="left",
                box=rich.box.SQUARE,
                expand=False,
                padding=(0, 0),
            )
        )

        # --- Return deployment ID ---
        return deployment_id


@app.command(rich_help_panel="Deploy")
async def run(
    deployment: Annotated[
        str,
        typer.Argument(
            help="Name or ID of the deployment to run",
            autocompletion=completions.complete_deployment,
        ),
    ],
    parameters: Annotated[
        list[str] | None,
        typer.Option(
            "--parameter",
            "-p",
            help="Function parameter in <NAME=VALUE> format (can be used multiple times)",
            rich_help_panel="Run",
            show_default=False,
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
    Run a deployment immediately

    Examples:
        $ prefect-cloud run flow_name/deployment_name
    """
    app.quiet = quiet

    parameters = parameters or []

    ui_url, _, _ = await auth.get_cloud_urls_or_login()
    func_kwargs = process_key_value_pairs(parameters, as_json=True)

    flow_run = await deployments.run(deployment, func_kwargs)
    flow_run_url = f"{ui_url}/runs/flow-run/{flow_run.id}"

    app.print(
        f"Started flow run [bold cyan]{flow_run.name}[/] 🚀\nView at:",
        Text(flow_run_url, style="link", justify="left"),
        soft_wrap=True,
    )

    async with await auth.get_prefect_cloud_client() as client:
        deployment_ = await deployments.get_deployment(deployment)
        work_pool = await client.read_work_pool_by_name(deployment_.work_pool_name)

    work_pool_url = f"{ui_url}/work-pools"
    if work_pool.is_paused:
        app.print(
            "\n",
            "[bold][orange1]Note:[/orange1][/bold] Your work pool is",
            "currently [bold]paused[/bold]. This will prevent the deployment "
            "from running until it is [bold]resumed[/bold].  Visit",
            Text(work_pool_url, style="link", justify="left"),
            "to resume the work pool.",
            soft_wrap=True,
        )


@app.command(rich_help_panel="Deploy")
async def schedule(
    deployment: Annotated[
        str,
        typer.Argument(
            help="Name or ID of the deployment to schedule",
            autocompletion=completions.complete_deployment,
        ),
    ],
    schedule: Annotated[
        str | None,
        typer.Argument(
            help="Cron schedule string or 'none' to unschedule",
        ),
    ],
    parameters: Annotated[
        list[str] | None,
        typer.Option(
            "--parameter",
            "-p",
            help="Function parameter in <NAME=VALUE> format (can be used multiple times)",
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
    Set a deployment to run on a schedule

    Examples:
        Run daily at midnight:
        $ prefect-cloud schedule flow_name/deployment_name "0 0 * * *"

        Run every hour:
        $ prefect-cloud schedule flow_name/deployment_name "0 * * * *"

        Remove schedule:
        $ prefect-cloud schedule flow_name/deployment_name none
    """
    app.quiet = quiet

    parameters = parameters or []

    func_kwargs = process_key_value_pairs(parameters, as_json=True)
    await deployments.schedule(deployment, schedule, func_kwargs)
    app.exit_with_success("[bold]✓[/] Deployment scheduled")


@app.command(rich_help_panel="Deploy")
async def unschedule(
    deployment: Annotated[
        str,
        typer.Argument(
            help="Name or ID of the deployment to remove schedules from",
        ),
    ],
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
    Remove deployment schedules
    """
    app.quiet = quiet

    await deployments.schedule(deployment, "none")
    app.exit_with_success("[bold]✓[/] Deployment unscheduled")


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
            app.print(f"Unknown schedule type: {type(schedule.schedule)}")
            description = "Unknown"

        return Text(f"{prefix} {description}", style=style)

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

    app.print(table)

    app.print(
        "* Cron cheatsheet: minute hour day-of-month month day-of-week",
        style="dim",
    )


@app.command(rich_help_panel="Deploy")
async def delete(
    deployment: Annotated[
        str,
        typer.Argument(
            help="Name or ID of the deployment to delete",
            autocompletion=completions.complete_deployment,
        ),
    ],
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
    Delete a deployment
    """
    app.quiet = quiet

    await deployments.delete(deployment)
    app.exit_with_success("[bold]✓[/] Deployment deleted")


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
