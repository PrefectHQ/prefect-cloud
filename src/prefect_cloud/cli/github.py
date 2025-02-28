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
    Setup Prefect Cloud GitHub integration
    """
    app.console.print("Setting up Prefect Cloud GitHub integration...")
    async with await get_prefect_cloud_client() as client:
        await install_github_app_interactively(client)
        repos = await client.get_github_repositories()
        repos_list = "\n".join([f"  - {repo}" for repo in repos])
        exit_with_success(
            f"Setup complete! You can now deploy from the following repositories:\n{repos_list}"
        )


@github_app.command()
async def ls():
    """
    List GitHub repositories connected to Prefect Cloud.
    """
    async with await get_prefect_cloud_client() as client:
        repos = await client.get_github_repositories()

        if not repos:
            exit_with_error(
                "No repositories found! "
                "Install the Prefect Cloud GitHub App with `prefect-cloud github setup`."
            )

        app.console.print("You can deploy from the following repositories:")
        for repo in repos:
            app.console.print(f"  - {repo}")
