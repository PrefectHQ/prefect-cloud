from pathlib import Path

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn
from typing_extensions import Annotated

from prefect.cli._utilities import exit_with_error
from prefect.cli.root import PrefectTyper
from prefect.client.base import ServerType, determine_server_type
from prefect.utilities.urls import url_for

from prefect_cloud.bundle import package_files
from prefect_cloud.dependencies import get_dependencies
from prefect_cloud.github import GitHubFileRef, get_github_raw_content
from prefect_cloud.client import (
    get_cloud_api_url,
    get_prefect_cloud_client,
)
from prefect_cloud.utilities.flows import get_parameter_schema_from_content

app = PrefectTyper()


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
async def git_deploy(
    filename: str,
    flow_func: str,
    dependencies: Annotated[
        list[str],
        typer.Option(
            "--dependencies",
            "-d",
            help="Dependencies to include. Can be a string (`package`), "
            "a comma separated string (`package1, package2`), "
            "a path to a requirements file, or a path to a pyproject.toml file."
            "Can be specified multiple times.",
        ),
    ] = None,
    env: Annotated[
        list[str],
        typer.Option(
            "--env",
            "-e",
            help="Environment variables to set in the format KEY=VALUE. Can be specified multiple times.",
        ),
    ] = None,
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
            exit_with_error(str(e))

        async with get_prefect_cloud_client() as client:
            task = progress.add_task("Inspecting code in github..", total=None)

            github_ref = GitHubFileRef.from_url(filename)
            raw_contents = await get_github_raw_content(github_ref)
            try:
                parameter_schema = get_parameter_schema_from_content(
                    raw_contents, flow_func
                )
            except ValueError:
                exit_with_error(
                    f"Could not find function '{flow_func}' in {github_ref.filepath}"
                )

            progress.update(task, description="Ensuring work pool exists...")
            work_pool = await client.ensure_managed_work_pool()

            progress.update(task, description="Deploying flow...")
            deployment_name = f"{flow_func}_deployment"

            pull_steps = [
                github_ref.pull_step,
                {
                    "prefect.deployments.steps.run_shell_script": {
                        "script": f"uv run https://raw.githubusercontent.com/PrefectHQ/prefect-cloud/refs/heads/main/src/prefect_cloud/pull_steps/flowify.py {github_ref.repo}-{github_ref.branch}/{github_ref.filepath} {flow_func}"
                    }
                },
            ]

            deployment_id = await client.create_managed_deployment(
                deployment_name,
                github_ref.filepath,
                flow_func,
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
        f"Run it with: \n $ prefect deployment run {flow_func}/{deployment_name}",
        style="blue",
    )


@app.command()
async def deploy(
    filename: str,
    flow_func: str,
    dependencies: Annotated[
        list[str],
        typer.Option(
            "--dependencies",
            "-d",
            help="Dependencies to include. Can be a string (`package`), "
            "a comma separated string (`package1, package2`), "
            "a path to a requirements file, or a path to a pyproject.toml file."
            "Can be specified multiple times.",
        ),
    ] = None,
    include: Annotated[
        list[str],
        typer.Option(
            "--include",
            "-i",
            help="Files or directories to include in the deployment. Can be specified multiple times.",
        ),
    ] = None,
    env: Annotated[
        list[str],
        typer.Option(
            "--env",
            "-e",
            help="Environment variables to set in the format KEY=VALUE. Can be specified multiple times.",
        ),
    ] = None,
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
            exit_with_error(str(e))

        async with get_prefect_cloud_client() as client:
            task = progress.add_task("Prefect-ifying your workflow...", total=None)

            raw_contents = Path(filename).read_text()
            try:
                parameter_schema = get_parameter_schema_from_content(
                    raw_contents, flow_func
                )
            except ValueError:
                exit_with_error(f"Could not find function '{flow_func}' in {filename}")

            progress.update(task, description="Uploading code to storage...")

            storage_id = await client.create_code_storage()

            code_bundle = package_files(filename, flow_func, include)
            await client.upload_code_to_storage(storage_id, code_bundle)

            progress.update(task, description="Ensuring work pool exists...")
            work_pool = await client.ensure_managed_work_pool()

            progress.update(task, description="Deploying flow...")
            deployment_name = f"{flow_func}_deployment"
            deployment_id = await client.create_managed_deployment(
                deployment_name,
                filename,
                flow_func,
                work_pool,
                pull_steps=[
                    {
                        "prefect.deployments.steps.run_shell_script": {
                            "script": f"uv run https://raw.githubusercontent.com/PrefectHQ/prefect-cloud/refs/heads/main/src/prefect_cloud/pull_steps/retrieve_code.py {storage_id}"
                        }
                    },
                    {
                        "prefect.deployments.steps.run_shell_script": {
                            "script": f"uv run https://raw.githubusercontent.com/PrefectHQ/prefect-cloud/refs/heads/main/src/prefect_cloud/pull_steps/flowify.py {filename} {flow_func}"
                        }
                    },
                ],
                parameter_schema=parameter_schema,
                job_variables={
                    "pip_packages": get_dependencies(dependencies or []),
                    "env": {"PREFECT_CLOUD_API_URL": get_cloud_api_url()} | env_vars,
                },
            )
            await client.set_deployment_id(storage_id, deployment_id)

    app.console.print(
        f"View deployment here: "
        f"\n ➜ [link={url_for('deployment', deployment_id)}]"
        f"{deployment_name}"
        f"[/link]",
        style="blue",
    )

    app.console.print(
        f"Run it with: \n $ prefect deployment run {flow_func}/{deployment_name}",
        style="blue",
    )
