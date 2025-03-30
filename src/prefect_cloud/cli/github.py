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
                    "[bold]✓[/] Prefect Cloud GitHub integration complete\n\n"
                    f"Connected repositories:\n{repos_list}\n\n"
                    "Deploy a function from your repo with:\n"
                    "prefect-cloud deploy <file.py:function> --from <github repo>"
                )
            else:
                app.exit_with_error(
                    "[bold]✗[/] No repositories found\n\n"
                    "This may mean:\n"
                    "• The integration was not successful, or\n"
                    "• The integration is still pending GitHub admin approval\n\n"
                    "Once approved, you’ll be able to deploy from your GitHub repos using:\n"
                    "prefect-cloud deploy <file.py:function> --from <github repo>"
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
                "[bold]✗[/] No repositories found\n\n"
                "This likely means:\n"
                "• The GitHub integration has not been set up, or\n"
                "• The integration is pending GitHub admin approval\n\n"
                "To get started:\n"
                "  Run: prefect-cloud github setup\n\n"
                "If the integration is pending:\n"
                "  Once a GitHub admin approves the installation, you can deploy from your repos using:\n"
                "  prefect-cloud deploy <file.py:function> --from <github repo>"
            )

        repos_list = "\n".join([f"- {repo}" for repo in repos])
        app.exit_with_success(
            f"Connected repositories:\n{repos_list}\n\n"
            f"Deploy a function from your repo with:\n"
            f"prefect-cloud deploy <file.py:function> --from <github repo>"
        )
