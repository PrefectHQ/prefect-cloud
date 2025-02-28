from prefect_cloud.auth import get_prefect_cloud_client
from prefect_cloud.cli.root import app
from prefect_cloud.cli.utilities import (
    PrefectCloudTyper,
    exit_with_error,
    exit_with_success,
)
from prefect_cloud.github import install_github_app_interactively

github_app = PrefectCloudTyper(help="Prefect Cloud + GitHub")
app.add_typer(github_app, name="github", rich_help_panel="Code Source")


@github_app.command()
async def setup():
    """
    Initialize a new GitHub integration.
    """
    app.console.print("Setting up Prefect Cloud GitHub integration...")
    async with await get_prefect_cloud_client() as client:
        await install_github_app_interactively(client)
    exit_with_success("Setup complete!")


@github_app.command()
async def ls():
    """
    Initialize a new GitHub integration.
    """
    async with await get_prefect_cloud_client() as client:
        repos = await client.get_github_repositories()

        if not repos:
            exit_with_error(
                "No repositories found! "
                "Configure the Prefect Cloud GitHub integration with `prefect-cloud github setup`."
            )

        for repo in repos:
            app.console.print(repo)
