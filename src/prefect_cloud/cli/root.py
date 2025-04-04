import typer

from prefect_cloud import __version__
from prefect_cloud.cli.utilities import PrefectCloudTyper

app = PrefectCloudTyper(
    rich_markup_mode=True,
    help="Deploy with Prefect Cloud",
    short_help="Deploy with Prefect Cloud",
)


def version_callback(value: bool) -> None:
    if value:
        print(__version__)
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Display the current version of prefect-cloud",
        callback=version_callback,
        is_eager=True,
    ),
) -> None:
    """
    Main callback for the CLI app.
    """
    pass
