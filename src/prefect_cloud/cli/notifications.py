import asyncio
from datetime import timedelta
from typing import Annotated

import typer
from prefect_cloud.schemas.actions import AutomationCreate
from prefect_cloud.schemas.objects import EventTrigger, SendEmailNotification
from prefect_cloud.auth import get_prefect_cloud_client
from prefect_cloud.cli import completions
from prefect_cloud.deployments import get_deployment
from prefect_cloud.cli.root import app
from prefect_cloud.cli.utilities import PrefectCloudTyper
from prefect_cloud.utilities.exceptions import ObjectNotFound

notifications_app = PrefectCloudTyper(
    help="Manage deployment notifications",
)
app.add_typer(notifications_app, name="notifications")


@notifications_app.command()
async def create(
    deployments: Annotated[
        list[str] | None,
        typer.Option(
            "--deployment",
            "-d",
            help="Name or ID of deployment to create notifications for (can be used multiple times)",
            autocompletion=completions.complete_deployment,
            show_default=False,
        ),
    ] = None,
    all_deployments: Annotated[
        bool,
        typer.Option(
            "--all",
            help="Send notifications for all deployments",
            show_default=False,
        ),
    ] = False,
    emails: Annotated[
        list[str] | None,
        typer.Option(
            "--email",
            "-e",
            help="Emails to send notifications (can be used multiple times)",
            show_default=False,
        ),
    ] = None,
    on_failure: Annotated[
        bool,
        typer.Option(
            "--on-failure",
            help="Send notifications on failure",
            show_default=False,
        ),
    ] = False,
    on_success: Annotated[
        bool,
        typer.Option(
            "--on-success",
            help="Send notifications on success",
            show_default=False,
        ),
    ] = False,
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
        # Expected State
        if on_failure and on_success:
            app.exit_with_error(
                "[bold]✗[/] Cannot specify both --on-failure and --on-success options"
            )

        if on_failure:
            expect = [
                "prefect.flow-run.Crashed",
                "prefect.flow-run.Failed",
                "prefect.flow-run.TimedOut",
            ]
            notify_on = "FAILURE"
        elif on_success:
            expect = ["prefect.flow-run.Completed"]
            notify_on = "SUCCESS"
        else:
            app.exit_with_error(
                "[bold]✗[/] One of --on-failure or --on-success options is required"
            )

        # Emails
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

        # Deployments
        if all_deployments and deployments:
            app.exit_with_error(
                "[bold]✗[/] Cannot specify both --all and --deployment options"
            )

        if all_deployments:
            name = f"Notify on {notify_on} for: all"
            resource_ids = ["prefect.deployment.*"]
            deployments_str = "all deployments"
        else:
            assert deployments
            deployment_objs = []
            for deployment in deployments:
                try:
                    deployment_obj = await get_deployment(deployment)
                except ObjectNotFound:
                    app.exit_with_error(
                        f"[bold]✗[/] Deployment {deployment} does not exist"
                    )
                else:
                    deployment_objs.append(deployment_obj)

            name = f"Notify on {notify_on} for: " + ", ".join(
                [deployment.name for deployment in deployment_objs]
            )
            resource_ids = [
                f"prefect.deployment.{deployment.id}" for deployment in deployment_objs
            ]
            deployments_str = ", ".join([d.name for d in deployment_objs])

        await client.create_automation(
            AutomationCreate(
                name=name,
                trigger=EventTrigger(
                    match={"prefect.resource.id": "prefect.flow-run.*"},
                    match_related={
                        "prefect.resource.role": "deployment",
                        "prefect.resource.id": resource_ids,
                    },
                    after=[],
                    expect=expect,
                    for_each=["prefect.resource.id"],
                    posture="Reactive",
                    threshold=1,
                    within=timedelta(seconds=0),
                ),
                actions=[
                    SendEmailNotification(
                        type="send-email-notification",
                        subject="{{ deployment.name}}: " + notify_on,
                        body=(
                            "Flow run {{ flow.name }}/{{ flow_run.name }} observed in state `{{ flow_run.state.name }}` "
                            "at {{ flow_run.state.timestamp }}.\n"
                            "Flow ID: {{ flow_run.flow_id }}\n"
                            "Flow run ID: {{ flow_run.id }}\n"
                            "Flow run URL: {{ flow_run|ui_url }}\n"
                            "State message: {{ flow_run.state.message }}"
                        ),
                        emails=emails,
                    )
                ],
            )
        )

        emails_str = ", ".join(sorted(emails))

        if notify_on == "FAILURE":
            notify_on_str = "[red][bold]FAILURE[/bold][/red]"
        else:
            notify_on_str = "[green][bold]SUCCESS[/bold][/green]"

        # TODO fix this?
        #  add labels?
        app.print(
            f"[dim]===============================[/]\n"
            f"[green][bold]✓ Notification created[/bold][/green]\n"
            f"[dim]-------------------------------[/]\n"
            f"→[bold]{emails_str}[/bold] will be notified when a "
            f"{notify_on_str} occurs for "
            # TODO FORMATTing messed up here
            # f"{'the deployment' if len(deployment_objs) == 1 else 'the deployments'}: "
            f"[bold]{deployments_str}[/bold]\n"
            f"[dim]===============================[/]"
        )


@notifications_app.command()
async def clear():
    async with await get_prefect_cloud_client() as client:
        automations = await client.list_automations()
        await asyncio.gather(*[client.delete_automation(a.id) for a in automations])

        app.print("[green][bold]✓ All Notifications deleted[/bold][/green]\n")
