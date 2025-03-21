from prefect_cloud.auth import get_prefect_cloud_client
from prefect_cloud.cli.root import app
from prefect_cloud.cli.utilities import (
    PrefectCloudTyper,
)
from prefect_cloud.github import install_github_app_interactively

github_app = PrefectCloudTyper(help="Prefect Cloud + GitHub")
app.add_typer(github_app, name="github", rich_help_panel="Code Source")


@github_app.command()
async def setup():
    """
    Setup Prefect Cloud GitHub integration
    """

    with app.create_progress() as progress:
        progress.add_task("Setting up Prefect Cloud GitHub integration...")
        async with await get_prefect_cloud_client() as client:
            await install_github_app_interactively(client)
            repos = await client.get_github_repositories()

            if repos:
                repos_list = "\n".join([f"  - {repo}" for repo in repos])
                app.exit_with_success(
                    f"[bold]✓[/] Prefect Cloud Github integration complete\n\n"
                    f"Connected repositories:\n"
                    f"{repos_list}"
                )
            else:
                app.exit_with_error(
                    "[bold]✗[/] No repositories found, integration may have unsuccessful"
                )


@github_app.command()
async def ls():
    """
    List GitHub repositories connected to Prefect Cloud.
    """
    async with await get_prefect_cloud_client() as client:
        repos = await client.get_github_repositories()

        if not repos:
            app.exit_with_error(
                "No repositories found.\n\n"
                "Install the Prefect Cloud GitHub App with:\n"
                "prefect-cloud github setup"
            )

        repos = await client.get_github_repositories()
        repos_list = "\n".join([f"- {repo}" for repo in repos])
        app.exit_with_success(f"Connected repositories:\n{repos_list}")
