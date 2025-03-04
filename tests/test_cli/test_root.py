from __future__ import annotations

import contextlib
import re
import textwrap
from typing import Iterable
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import readchar
from click.testing import Result
from rich.console import Console
from typer.testing import CliRunner

from prefect_cloud.cli.root import app
from prefect_cloud.github import FileNotFound
from prefect_cloud.schemas.objects import WorkPool
from prefect_cloud.schemas.responses import DeploymentResponse


def check_contains(cli_result: Result, content: str, should_contain: bool) -> None:
    """
    Utility function to see if content is or is not in a CLI result.

    Args:
        should_contain: if True, checks that content is in cli_result,
            if False, checks that content is not in cli_result
    """
    output = cli_result.stdout.strip()
    content = textwrap.dedent(content).strip()

    if should_contain:
        section_heading = "------ desired content ------"
    else:
        section_heading = "------ undesired content ------"

    print(section_heading)
    print(content)
    print()

    if len(content) > 20:
        display_content = content[:20] + "..."
    else:
        display_content = content

    if should_contain:
        assert content in output, (
            f"Desired contents {display_content!r} not found in CLI output"
        )
    else:
        assert content not in output, (
            f"Undesired contents {display_content!r} found in CLI output"
        )


def invoke_and_assert(
    command: str | list[str],
    user_input: str | None = None,
    prompts_and_responses: list[tuple[str, str] | tuple[str, str, str]] | None = None,
    expected_output: str | None = None,
    expected_output_contains: str | Iterable[str] | None = None,
    expected_output_does_not_contain: str | Iterable[str] | None = None,
    expected_line_count: int | None = None,
    expected_code: int | None = 0,
    echo: bool = True,
    temp_dir: str | None = None,
) -> Result:
    """
    Test utility for the Prefect CLI application, asserts exact match with CLI output.

    Args:
        command: Command passed to the Typer CliRunner
        user_input: User input passed to the Typer CliRunner when running interactive
            commands.
        expected_output: Used when you expect the CLI output to be an exact match with
            the provided text.
        expected_output_contains: Used when you expect the CLI output to contain the
            string or strings.
        expected_output_does_not_contain: Used when you expect the CLI output to not
            contain the string or strings.
        expected_code: 0 if we expect the app to exit cleanly, else 1 if we expect
            the app to exit with an error.
        temp_dir: if provided, the CLI command will be run with this as its present
            working directory.
    """
    prompts_and_responses = prompts_and_responses or []
    runner = CliRunner()
    if temp_dir:
        ctx = runner.isolated_filesystem(temp_dir=temp_dir)
    else:
        ctx = contextlib.nullcontext()

    if user_input and prompts_and_responses:
        raise ValueError("Cannot provide both user_input and prompts_and_responses")

    if prompts_and_responses:
        user_input = (
            ("\n".join(response for (_, response, *_) in prompts_and_responses) + "\n")
            .replace("↓", readchar.key.DOWN)
            .replace("↑", readchar.key.UP)
        )

    with ctx:
        result = runner.invoke(app, command, catch_exceptions=False, input=user_input)

    if echo:
        print("\n------ CLI output ------")
        print(result.stdout)

    if expected_code is not None:
        assertion_error_message = (
            f"Expected code {expected_code} but got {result.exit_code}\n"
            "Output from CLI command:\n"
            "-----------------------\n"
            f"{result.stdout}"
        )
        assert result.exit_code == expected_code, assertion_error_message

    if expected_output is not None:
        output = result.stdout.strip()
        expected_output = textwrap.dedent(expected_output).strip()

        compare_string = (
            "------ expected ------\n"
            f"{expected_output}\n"
            "------ actual ------\n"
            f"{output}\n"
            "------ end ------\n"
        )
        assert output == expected_output, compare_string

    if prompts_and_responses:
        output = result.stdout.strip()
        cursor = 0

        for item in prompts_and_responses:
            prompt = item[0]
            selected_option = item[2] if len(item) == 3 else None

            prompt_re = rf"{re.escape(prompt)}.*?"
            if not selected_option:
                # If we're not prompting for a table, then expect that the
                # prompt ends with a colon.
                prompt_re += ":"

            match = re.search(prompt_re, output[cursor:])
            if not match:
                raise AssertionError(f"Prompt '{prompt}' not found in CLI output")
            cursor = cursor + match.end()

            if selected_option:
                option_re = re.escape(f"│ >  │ {selected_option}")
                match = re.search(option_re, output[cursor:])
                if not match:
                    raise AssertionError(
                        f"Option '{selected_option}' not found after prompt '{prompt}'"
                    )
                cursor = cursor + match.end()

    if expected_output_contains is not None:
        if isinstance(expected_output_contains, str):
            check_contains(result, expected_output_contains, should_contain=True)
        else:
            for contents in expected_output_contains:
                check_contains(result, contents, should_contain=True)

    if expected_output_does_not_contain is not None:
        if isinstance(expected_output_does_not_contain, str):
            check_contains(
                result, expected_output_does_not_contain, should_contain=False
            )
        else:
            for contents in expected_output_does_not_contain:
                check_contains(result, contents, should_contain=False)

    if expected_line_count is not None:
        line_count = len(result.stdout.splitlines())
        assert expected_line_count == line_count, (
            f"Expected {expected_line_count} lines of CLI output, only"
            f" {line_count} lines present"
        )

    return result


@contextlib.contextmanager
def temporary_console_width(console: Console, width: int):
    original = console.width

    try:
        console._width = width  # type: ignore
        yield
    finally:
        console._width = original  # type: ignore


def test_deploy_command_basic():
    """Test basic deployment without running"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        # Setup mock client and responses
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        # Mock auth responses
        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        # Mock auth.get_cloud_urls_or_login
        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            # Mock GitHub content retrieval
            with patch(
                "prefect_cloud.github.GitHubRepo.get_file_contents"
            ) as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function():
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test.py:test_function",
                        "--from",
                        "github.com/owner/repo",
                        "--with",
                        "prefect",
                    ],
                    expected_code=0,
                    expected_output_contains=[
                        "Deployed test_function",
                        "prefect-cloud run test_function/test_function",
                        "prefect-cloud schedule test_function/test_function '<CRON SCHEDULE>'",
                        "https://ui.url/deployments/deployment/test-deployment-id",
                    ],
                )

                # Verify the deployment was created with expected args
                client.create_managed_deployment.assert_called_once()
                call_kwargs = client.create_managed_deployment.call_args[1]
                assert call_kwargs["deployment_name"] == "test_function"
                assert call_kwargs["filepath"] == "test.py"
                assert call_kwargs["function"] == "test_function"
                assert call_kwargs["work_pool_name"] == "test-pool"


def test_deploy_private_repo_without_credentials():
    """Test deployment fails appropriately when accessing private repo without credentials"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        # Mock GitHub token retrieval to return None (not installed)
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch(
                "prefect_cloud.github.GitHubRepo.get_file_contents"
            ) as mock_content:
                mock_content.side_effect = FileNotFound()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "src/flows/test.py:test_function",
                        "--from",
                        "github.com/owner/repo",
                        "--with",
                        "prefect",
                    ],
                    expected_code=1,
                    expected_output_contains=[
                        "Unable to access file",
                        "src/flows/test.py",
                        "owner/repo",
                        "Make sure the file exists",
                        "accessible",
                        "private repository",
                        "GitHub App",
                        "prefect-cloud github setup",
                        "--credentials",
                    ],
                )


def test_deploy_with_env_vars():
    """Test deployment with environment variables"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        # Mock GitHub token retrieval to return None (using public repo path)
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            # Mock the pull steps generation to return a simple single step
            with patch(
                "prefect_cloud.github.GitHubRepo.public_repo_pull_steps"
            ) as mock_pull_steps:
                mock_pull_steps.return_value = [
                    {
                        "prefect.deployments.steps.git_clone": {
                            "id": "git-clone",
                            "repository": "https://github.com/owner/repo.git",
                            "branch": "main",
                        }
                    }
                ]

                with patch(
                    "prefect_cloud.github.GitHubRepo.get_file_contents"
                ) as mock_content:
                    mock_content.return_value = textwrap.dedent("""
                        def test_function():
                            pass
                    """).lstrip()

                    invoke_and_assert(
                        command=[
                            "deploy",
                            "test.py:test_function",
                            "--from",
                            "github.com/owner/repo",
                            "--with",
                            "prefect",
                            "--env",
                            "API_KEY=secret",
                            "--env",
                            "DEBUG=true",
                        ],
                        expected_code=0,
                        expected_output_contains=[
                            "Deployed test_function",
                            "prefect-cloud run test_function/test_function",
                            "prefect-cloud schedule test_function/test_function '<CRON SCHEDULE>'",
                            "https://ui.url/deployments/deployment/test-deployment-id",
                            "github.com/owner/repo",
                        ],
                    )

                    # Verify environment variables were passed correctly
                    client.create_managed_deployment.assert_called_once()
                    job_variables = client.create_managed_deployment.call_args[1][
                        "job_variables"
                    ]
                    assert job_variables["env"]["API_KEY"] == "secret"
                    assert job_variables["env"]["DEBUG"] == "true"


def test_deploy_with_parameters():
    """Test deployment with parameters"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        # Mock GitHub token retrieval to return None (using public repo path)
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            # Mock the pull steps generation to return a simple single step
            with patch(
                "prefect_cloud.github.GitHubRepo.public_repo_pull_steps"
            ) as mock_pull_steps:
                mock_pull_steps.return_value = [
                    {
                        "prefect.deployments.steps.git_clone": {
                            "id": "git-clone",
                            "repository": "https://github.com/owner/repo.git",
                            "branch": "main",
                        }
                    }
                ]

                with patch(
                    "prefect_cloud.github.GitHubRepo.get_file_contents"
                ) as mock_content:
                    mock_content.return_value = textwrap.dedent("""
                        def test_function():
                            pass
                    """).lstrip()

                    invoke_and_assert(
                        command=[
                            "deploy",
                            "test.py:test_function",
                            "--from",
                            "github.com/owner/repo",
                            "--with",
                            "prefect",
                            "-p",
                            "x=1",
                            "-p",
                            "y=test",
                            "-p",
                            "z=false",
                        ],
                        expected_code=0,
                        expected_output_contains=[
                            "Deployed test_function",
                            "prefect-cloud run test_function/test_function",
                            "prefect-cloud schedule test_function/test_function '<CRON SCHEDULE>'",
                            "https://ui.url/deployments/deployment/test-deployment-id",
                            "github.com/owner/repo",
                        ],
                    )

                    # Verify environment variables were passed correctly
                    client.create_managed_deployment.assert_called_once()
                    parameters = client.create_managed_deployment.call_args[1][
                        "parameters"
                    ]
                    # the int should have been parsed properly
                    assert parameters["x"] == 1
                    assert parameters["y"] == "test"
                    assert parameters["z"] is False


def test_deploy_with_invalid_parameters():
    """Test deployment with parameters"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            invoke_and_assert(
                command=[
                    "deploy",
                    "test.py:test_function",
                    "--from",
                    "github.com/owner/repo",
                    "--with",
                    "prefect",
                    "-p",
                    "x",
                ],
                expected_code=1,
                expected_output_contains=["Invalid key value pairs: ['x']"],
            )


def test_deploy_with_private_repo_credentials():
    """Test deployment with credentials for private repository"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")
        client.create_credentials_secret = AsyncMock()

        # Mock GitHub token retrieval to return None (not installed)
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch(
                "prefect_cloud.github.GitHubRepo.get_file_contents"
            ) as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function():
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test.py:test_function",
                        "--from",
                        "github.com/owner/repo",
                        "--with",
                        "prefect",
                        "--credentials",
                        "github_token",
                    ],
                    expected_code=0,
                    expected_output_contains=[
                        "Deployed test_function",
                        "prefect-cloud run test_function/test_function",
                        "prefect-cloud schedule test_function/test_function '<CRON SCHEDULE>'",
                        "https://ui.url/deployments/deployment/test-deployment-id",
                        "github.com/owner/repo",
                    ],
                )

                # Verify credentials were stored with new parameter naming
                client.create_credentials_secret.assert_called_once_with(
                    name="owner-repo-credentials", credentials="github_token"
                )


def test_run_invalid_parameters():
    """Test deployment fails with invalid parameter format"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            invoke_and_assert(
                command=[
                    "run",
                    "test_deployment",
                    "--parameter",
                    "invalid_param",  # Missing = sign
                ],
                expected_code=1,
                expected_output_contains="Invalid key value pairs",
            )


def test_deploy_function_not_found():
    """Test deployment fails when function doesn't exist in file"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch(
                "prefect_cloud.github.GitHubRepo.get_file_contents"
            ) as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def other_function():
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test.py:test_function",
                        "--from",
                        "github.com/owner/repo",
                        "--with",
                        "prefect",
                    ],
                    expected_code=1,
                    expected_output_contains="Could not find function 'test_function'",
                )


def test_run():
    """Test running a deployment"""
    mock_deployment = DeploymentResponse(
        id=uuid4(),
        flow_id=uuid4(),
        name="test-deployment",
        work_pool_name="test-pool",
        schedules=[],
    )

    with (
        patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client,
        patch("prefect_cloud.deployments.get_deployment", return_value=mock_deployment),
    ):
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.read_work_pool_by_name = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch("prefect_cloud.cli.root.deployments.run") as mock_run:
                # Create a proper mock for the flow run
                flow_run_mock = MagicMock()
                flow_run_mock.id = "test-run-id"
                flow_run_mock.name = "test-run"

                mock_run.return_value = flow_run_mock

                invoke_and_assert(
                    command=[
                        "run",
                        "test_deployment",
                        "--parameter",
                        "x=1",
                        "--parameter",
                        "y=test",
                    ],
                    expected_code=0,
                    expected_output_contains=[
                        "Started flow run test-run! 🚀",
                        "View: https://ui.url/runs/flow-run/test-run-id",
                    ],
                )

                # Verify the deployment was run with parameters
                mock_run.assert_called_once_with(
                    "test_deployment", {"x": 1, "y": "test"}
                )


def test_deploy_with_dependencies():
    """Test deployment with python dependencies"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        # Mock GitHub token retrieval to return None (using public repo path)
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            # Mock the pull steps generation to return a simple single step
            with patch(
                "prefect_cloud.github.GitHubRepo.public_repo_pull_steps"
            ) as mock_pull_steps:
                mock_pull_steps.return_value = [
                    {
                        "prefect.deployments.steps.git_clone": {
                            "id": "git-clone",
                            "repository": "https://github.com/owner/repo.git",
                            "branch": "main",
                        }
                    }
                ]

                with patch(
                    "prefect_cloud.github.GitHubRepo.get_file_contents"
                ) as mock_content:
                    mock_content.return_value = textwrap.dedent("""
                        def test_function():
                            pass
                    """).lstrip()

                    invoke_and_assert(
                        command=[
                            "deploy",
                            "test.py:test_function",
                            "--from",
                            "github.com/owner/repo",
                            "--with",
                            "requests>=2",
                            "--with",
                            "pandas>=1",
                        ],
                        expected_code=0,
                        expected_output_contains=[
                            "Deployed test_function",
                            "prefect-cloud run test_function/test_function",
                            "prefect-cloud schedule test_function/test_function '<CRON SCHEDULE>'",
                            "https://ui.url/deployments/deployment/test-deployment-id",
                            "github.com/owner/repo",
                        ],
                    )

                    # Verify deployment was created with correct pull steps
                    client.create_managed_deployment.assert_called_once()
                    deployment = client.create_managed_deployment.call_args[1]

                    # We don't want to pass them as EXTRA_PIP_PACKAGES, because that happens
                    # before logging can be set up, so any problems with the dependencies
                    # will be obscured to users.
                    assert "pip_packages" not in deployment["job_variables"]

                    pull_steps = deployment["pull_steps"]
                    assert len(pull_steps) == 2
                    assert pull_steps[1] == {
                        "prefect.deployments.steps.run_shell_script": {
                            "directory": "{{ git-clone.directory }}",
                            "script": "uv pip install 'requests>=2' 'pandas>=1'",
                        }
                    }


def test_deploy_with_requirements_file():
    """Test deployment with a requirements file"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        # Mock GitHub token retrieval to return None (using public repo path)
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            # Mock the pull steps generation to return a simple single step
            with patch(
                "prefect_cloud.github.GitHubRepo.public_repo_pull_steps"
            ) as mock_pull_steps:
                mock_pull_steps.return_value = [
                    {
                        "prefect.deployments.steps.git_clone": {
                            "id": "git-clone",
                            "repository": "https://github.com/owner/repo.git",
                            "branch": "main",
                        }
                    }
                ]

                with patch(
                    "prefect_cloud.github.GitHubRepo.get_file_contents"
                ) as mock_content:
                    mock_content.return_value = textwrap.dedent("""
                        def test_function():
                            pass
                    """).lstrip()

                    invoke_and_assert(
                        command=[
                            "deploy",
                            "test.py:test_function",
                            "--from",
                            "github.com/owner/repo",
                            "--with-requirements",
                            "requirements.txt",
                        ],
                        expected_code=0,
                        expected_output_contains=[
                            "Deployed test_function",
                            "prefect-cloud run test_function/test_function",
                            "prefect-cloud schedule test_function/test_function '<CRON SCHEDULE>'",
                            "https://ui.url/deployments/deployment/test-deployment-id",
                            "github.com/owner/repo",
                        ],
                    )

                    # Verify deployment was created with correct pull steps
                    client.create_managed_deployment.assert_called_once()
                    pull_steps = client.create_managed_deployment.call_args[1][
                        "pull_steps"
                    ]
                    assert len(pull_steps) == 2
                    assert pull_steps[1] == {
                        "prefect.deployments.steps.run_shell_script": {
                            "directory": "{{ git-clone.directory }}",
                            "script": "uv pip install -r requirements.txt",
                        }
                    }


def test_deploy_with_github_app():
    """Test deployment using GitHub App token"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        # Mock GitHub token retrieval to return a token (GitHub App installed)
        github_token = "github-app-token-123"
        client.get_github_token = AsyncMock(return_value=github_token)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            # Mock the GitHub App pull steps generation
            with patch(
                "prefect_cloud.github.GitHubRepo.private_repo_via_github_app_pull_steps"
            ) as mock_pull_steps:
                mock_pull_steps.return_value = [
                    {
                        "prefect.deployments.steps.run_shell_script": {
                            "id": "get-github-token",
                            "script": "echo 'get-token-script'",
                        }
                    },
                    {
                        "prefect.deployments.steps.git_clone": {
                            "id": "git-clone",
                            "repository": "https://x-access-token:{{ get-github-token.stdout }}@github.com/owner/repo.git",
                            "branch": "main",
                        }
                    },
                ]

                with patch(
                    "prefect_cloud.github.GitHubRepo.get_file_contents"
                ) as mock_content:
                    # Mock that the file can be accessed with the GitHub App token
                    mock_content.return_value = textwrap.dedent("""
                        def test_function():
                            pass
                    """).lstrip()

                    invoke_and_assert(
                        command=[
                            "deploy",
                            "test.py:test_function",
                            "--from",
                            "github.com/owner/repo",
                        ],
                        expected_code=0,
                        expected_output_contains=[
                            "Deployed test_function",
                            "prefect-cloud run test_function/test_function",
                            "prefect-cloud schedule test_function/test_function '<CRON SCHEDULE>'",
                            "https://ui.url/deployments/deployment/test-deployment-id",
                            "github.com/owner/repo",
                        ],
                    )

                    # Verify the GitHub App token was requested
                    client.get_github_token.assert_called_once_with("owner", "repo")

                    # Verify the file contents were fetched with the token
                    mock_content.assert_called_once_with("test.py", github_token)

                    # Verify deployment was created with correct GitHub App pull steps
                    client.create_managed_deployment.assert_called_once()
                    pull_steps = client.create_managed_deployment.call_args[1][
                        "pull_steps"
                    ]
                    assert len(pull_steps) == 2
                    assert (
                        pull_steps[0]["prefect.deployments.steps.run_shell_script"][
                            "id"
                        ]
                        == "get-github-token"
                    )
                    assert pull_steps[1]["prefect.deployments.steps.git_clone"][
                        "repository"
                    ].startswith("https://x-access-token")


def test_deploy_with_quiet_flag():
    """Test deployment with quiet flag suppresses output"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        # Mock GitHub token retrieval to return None (using public repo path)
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch(
                "prefect_cloud.github.GitHubRepo.get_file_contents"
            ) as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function():
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test.py:test_function",
                        "--from",
                        "github.com/owner/repo",
                        "--quiet",
                    ],
                    expected_code=0,
                    expected_output_does_not_contain=[
                        "Deployed test_function",
                        "prefect-cloud run test_function/test_function",
                        "prefect-cloud schedule test_function/test_function '<CRON SCHEDULE>'",
                        "https://ui.url/deployments/deployment/test-deployment-id",
                    ],
                )

                # Verify deployment was still created despite quiet output
                client.create_managed_deployment.assert_called_once()
                deployment = client.create_managed_deployment.call_args[1]
                assert deployment["deployment_name"] == "test_function"
                assert deployment["filepath"] == "test.py"
                assert deployment["function"] == "test_function"


def test_deploy_programmatic_invocation():
    """Test programmatic invocation of deploy command returns the deployment ID"""
    from prefect_cloud.cli.root import deploy

    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        deployment_id = uuid4()
        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value=deployment_id)

        # Mock GitHub token retrieval
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch(
                "prefect_cloud.github.GitHubRepo.get_file_contents"
            ) as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function():
                        pass
                """).lstrip()

                # Call deploy function programmatically
                result = deploy(
                    function="test.py:test_function",
                    repo="github.com/owner/repo",
                    credentials=None,
                    dependencies=[],
                    with_requirements=None,
                    env=[],
                    parameters=[],
                    quiet=True,
                )

                # Verify result is the UUID returned by create_managed_deployment
                assert result == deployment_id

                # Verify deployment was created with expected parameters
                client.create_managed_deployment.assert_called_once()
                deployment = client.create_managed_deployment.call_args[1]
                assert deployment["deployment_name"] == "test_function"
                assert deployment["filepath"] == "test.py"
                assert deployment["function"] == "test_function"


def test_deploy_with_custom_names():
    """Test deployment with custom flow and deployment names"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        # Mock GitHub token retrieval to return None (using public repo path)
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch(
                "prefect_cloud.github.GitHubRepo.get_file_contents"
            ) as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function():
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test.py:test_function",
                        "--from",
                        "github.com/owner/repo",
                        "--name",
                        "custom-deployment-name",
                        "--flow-name",
                        "custom-flow-name",
                    ],
                    expected_code=0,
                    expected_output_contains=[
                        "Deployed custom-deployment-name",
                        "prefect-cloud run test_function/custom-deployment-name",
                        "prefect-cloud schedule test_function/custom-deployment-name '<CRON SCHEDULE>'",
                        "https://ui.url/deployments/deployment/test-deployment-id",
                    ],
                )

                # Verify the deployment was created with custom names
                client.create_managed_deployment.assert_called_once()
                call_kwargs = client.create_managed_deployment.call_args[1]
                assert call_kwargs["deployment_name"] == "custom-deployment-name"
                assert call_kwargs["flow_name"] == "custom-flow-name"


def test_deploy_programmatic_with_custom_names():
    """Test programmatic invocation of deploy command with custom names"""
    from prefect_cloud.cli.root import deploy

    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        deployment_id = uuid4()
        client.ensure_managed_work_pool = AsyncMock(
            return_value=WorkPool(
                type="prefect:managed", name="test-pool", is_paused=False
            )
        )
        client.create_managed_deployment = AsyncMock(return_value=deployment_id)

        # Mock GitHub token retrieval
        client.get_github_token = AsyncMock(return_value=None)

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch(
                "prefect_cloud.github.GitHubRepo.get_file_contents"
            ) as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function():
                        pass
                """).lstrip()

                # Call deploy function programmatically with custom names
                result = deploy(
                    function="test.py:test_function",
                    repo="github.com/owner/repo",
                    credentials=None,
                    dependencies=[],
                    with_requirements=None,
                    env=[],
                    parameters=[],
                    deployment_name="custom-deployment-name",
                    flow_name="custom-flow-name",
                    quiet=True,
                )

                # Verify result is the UUID returned by create_managed_deployment
                assert result == deployment_id

                # Verify deployment was created with custom names
                client.create_managed_deployment.assert_called_once()
                call_kwargs = client.create_managed_deployment.call_args[1]
                assert call_kwargs["deployment_name"] == "custom-deployment-name"
                assert call_kwargs["flow_name"] == "custom-flow-name"
