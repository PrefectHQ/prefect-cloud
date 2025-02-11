from __future__ import annotations

import contextlib
import re
import textwrap
from typing import Iterable
from unittest.mock import AsyncMock, MagicMock, patch

import readchar
from rich.console import Console
from typer.testing import CliRunner, Result

from prefect_cloud.cli.root import app
from prefect_cloud.github import FileNotFound


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
        client.ensure_managed_work_pool = AsyncMock(return_value="test-pool")
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        # Mock auth.get_cloud_urls_or_login
        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            # Mock GitHub content retrieval
            with patch("prefect_cloud.cli.root.get_github_raw_content") as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function():
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test_function",
                        "--from",
                        "https://github.com/owner/repo/blob/main/test.py",
                        "--with",
                        "prefect",
                    ],
                    expected_code=0,
                    expected_output_contains=[
                        "View deployment here",
                        "Run it with: ",
                        "prefect-cloud run test_function/test_function",
                    ],
                )

                # Verify the deployment was created with expected args
                client.create_managed_deployment.assert_called_once()
                call_args = client.create_managed_deployment.call_args[0]
                assert call_args[0] == "test_function"  # deployment name
                assert call_args[1] == "test.py"  # filepath
                assert call_args[2] == "test_function"  # function name
                assert call_args[3] == "test-pool"  # work pool


def test_deploy_and_run():
    """Test deployment with immediate run"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        # Mock necessary responses
        client.ensure_managed_work_pool = AsyncMock(return_value="test-pool")
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")
        client.create_flow_run_from_deployment_id = AsyncMock(
            return_value=MagicMock(id="test-run-id", name="test-run")
        )

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch("prefect_cloud.cli.root.get_github_raw_content") as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function(x: int):
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test_function",
                        "--from",
                        "https://github.com/owner/repo/blob/main/test.py",
                        "--with",
                        "prefect",
                        "--run",
                        "--parameters",
                        "x=1",
                    ],
                    expected_code=0,
                    expected_output_contains=[
                        "View deployment here",
                        "View flow run here",
                    ],
                )

                # Verify flow run was created
                client.create_flow_run_from_deployment_id.assert_called_once_with(
                    "test-deployment-id", {"x": "1"}
                )


def test_deploy_private_repo_without_credentials():
    """Test deployment fails appropriately when accessing private repo without credentials"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch("prefect_cloud.cli.root.get_github_raw_content") as mock_content:
                mock_content.side_effect = FileNotFound()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test_function",
                        "--from",
                        "https://github.com/owner/repo/blob/main/test.py",
                        "--with",
                        "prefect",
                    ],
                    expected_code=1,
                    expected_output_contains=("Unable to access file in Github."),
                )


def test_deploy_with_env_vars():
    """Test deployment with environment variables"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(return_value="test-pool")
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch("prefect_cloud.cli.root.get_github_raw_content") as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function():
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test_function",
                        "--from",
                        "https://github.com/owner/repo/blob/main/test.py",
                        "--with",
                        "prefect",
                        "--env",
                        "API_KEY=secret",
                        "--env",
                        "DEBUG=true",
                    ],
                    expected_code=0,
                    expected_output_contains="View deployment here",
                )

                # Verify environment variables were passed correctly
                client.create_managed_deployment.assert_called_once()
                job_variables = client.create_managed_deployment.call_args[1][
                    "job_variables"
                ]
                assert job_variables["env"]["API_KEY"] == "secret"
                assert job_variables["env"]["DEBUG"] == "true"


def test_deploy_with_private_repo_credentials():
    """Test deployment with credentials for private repository"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        client.ensure_managed_work_pool = AsyncMock(return_value="test-pool")
        client.create_managed_deployment = AsyncMock(return_value="test-deployment-id")
        client.create_credentials_secret = AsyncMock()

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            with patch("prefect_cloud.cli.root.get_github_raw_content") as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def test_function():
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test_function",
                        "--from",
                        "https://github.com/owner/repo/blob/main/test.py",
                        "--with",
                        "prefect",
                        "--credentials",
                        "github_token",
                    ],
                    expected_code=0,
                    expected_output_contains="View deployment here",
                )

                # Verify credentials were stored
                client.create_credentials_secret.assert_called_once_with(
                    "owner-repo-credentials", "github_token"
                )


def test_deploy_invalid_parameters():
    """Test deployment fails with invalid parameter format"""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            invoke_and_assert(
                command=[
                    "deploy",
                    "test_function",
                    "--from",
                    "https://github.com/owner/repo/blob/main/test.py",
                    "--with",
                    "prefect",
                    "--run",
                    "--parameters",
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

            with patch("prefect_cloud.cli.root.get_github_raw_content") as mock_content:
                mock_content.return_value = textwrap.dedent("""
                    def other_function():
                        pass
                """).lstrip()

                invoke_and_assert(
                    command=[
                        "deploy",
                        "test_function",
                        "--from",
                        "https://github.com/owner/repo/blob/main/test.py",
                        "--with",
                        "prefect",
                    ],
                    expected_code=1,
                    expected_output_contains="Could not find function 'test_function'",
                )
