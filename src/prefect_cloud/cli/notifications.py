from typing import Annotated

import typer
from prefect_cloud.auth import get_prefect_cloud_client
from prefect_cloud.cli import completions
from prefect_cloud.deployments import get_deployment
from prefect_cloud.cli.root import app


@app.command(rich_help_panel="Notifications")
async def notify(
    deployment_: Annotated[
        str,
        typer.Argument(
            help="Name or ID of the deployment to schedule",
            autocompletion=completions.complete_deployment,
        ),
    ],
    emails: Annotated[
        list[str] | None,
        typer.Option(
            "--email",
            "-e",
            help="Emails to send notifications (can be used multiple times)",
            show_default=False,
        ),
    ] = None,
    exclude_me: Annotated[
        bool,
        typer.Option(
            "--exclude-me",
            help="Exclude the current user from receiving notifications",
            show_default=False,
        ),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Suppress output",
        ),
    ] = False,
):
    app.quiet = quiet

    async with await get_prefect_cloud_client() as client:
        emails = emails or []

        if not exclude_me:
            me = await client.me()
            if me.email:
                emails.append(me.email)
                emails = list(set(emails))

        if not emails:
            app.exit_with_error(
                "[bold]✗[/] One or more email addresses are required to configure notifications"
            )

        deployment = await get_deployment(deployment_)

        deployment_failed_automation = {
            "name": f"{deployment.name} Failed".title(),
            "trigger": {
                "type": "event",
                "match": {"prefect.resource.id": "prefect.flow-run.*"},
                "match_related": {
                    "prefect.resource.role": "deployment",
                    "prefect.resource.id": [f"prefect.deployment.{deployment.id}"],
                },
                "after": [],
                "expect": [
                    "prefect.flow-run.Crashed",
                    "prefect.flow-run.Failed",
                    "prefect.flow-run.TimedOut",
                ],
                "for_each": ["prefect.resource.id"],
                "posture": "Reactive",
                "threshold": 1,
                "within": 0,
            },
            "actions": [
                {
                    "type": "send-email-notification",
                    "subject": "{{ deployment.name}} failed",
                    "body": "Flow run {{ flow.name }}/{{ flow_run.name }} observed in state `{{ flow_run.state.name }}` "
                    "at {{ flow_run.state.timestamp }}.\n"
                    "Flow ID: {{ flow_run.flow_id }}\n"
                    "Flow run ID: {{ flow_run.id }}\n"
                    "Flow run URL: {{ flow_run|ui_url }}\n"
                    "State message: {{ flow_run.state.message }}",
                    "emails": emails,
                }
            ],
        }

        await client.create_automation(deployment_failed_automation)
