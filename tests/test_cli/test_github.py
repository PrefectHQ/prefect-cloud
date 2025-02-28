from unittest.mock import AsyncMock, patch

from tests.test_cli.test_root import invoke_and_assert


def test_github_setup():
    """Test the GitHub setup command."""
    with patch(
        "prefect_cloud.cli.github.install_github_app_interactively"
    ) as mock_install:
        # Mock the installation function
        mock_install.return_value = None

        # Run the CLI command
        invoke_and_assert(
            command=["github", "setup"],
            expected_code=0,
            expected_output_contains=[
                "Setting up Prefect Cloud GitHub integration...",
                "Setup complete!",
            ],
        )

        # Verify the installation function was called
        mock_install.assert_called_once()


def test_github_ls_with_repositories():
    """Test the GitHub ls command when repositories are available."""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        # Setup mock client
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        # Mock repositories
        test_repos = ["owner/repo1", "owner/repo2", "owner/repo3"]
        client.get_github_repositories.return_value = test_repos

        # Run the CLI command
        invoke_and_assert(
            command=["github", "ls"],
            expected_code=0,
            expected_output_contains=test_repos,
        )

        # Verify the get_github_repositories function was called
        client.get_github_repositories.assert_called_once()


def test_github_ls_no_repositories():
    """Test the GitHub ls command when no repositories are available."""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        # Setup mock client
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client

        # Mock empty repositories list
        client.get_github_repositories.return_value = []

        # Run the CLI command
        invoke_and_assert(
            command=["github", "ls"],
            expected_code=1,  # Should exit with error
            expected_output_contains=[
                "No repositories found!",
                "Configure the Prefect Cloud GitHub integration with `prefect-cloud github setup`.",
            ],
        )

        # Verify the get_github_repositories function was called
        client.get_github_repositories.assert_called_once()


def test_github_ls_exception_handling():
    """Test the GitHub ls command when an exception occurs."""
    with patch("prefect_cloud.auth.get_prefect_cloud_client") as mock_client:
        # Setup mock client to raise an exception
        client = AsyncMock()
        mock_client.return_value.__aenter__.return_value = client
        client.get_github_repositories.side_effect = Exception("Connection error")

        # Run the CLI command
        invoke_and_assert(
            command=["github", "ls"],
            expected_code=1,  # Should exit with error
            expected_output_contains="Connection error",
        )

        # Verify the get_github_repositories function was called
        client.get_github_repositories.assert_called_once()
