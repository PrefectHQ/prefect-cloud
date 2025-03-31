import pytest
from unittest.mock import AsyncMock

from tests.test_cli.test_root import invoke_and_assert


@pytest.fixture
def mock_outgoing_calls(monkeypatch):
    """Fixture to mock GitHub setup dependencies."""
    install_mock = AsyncMock()
    client_mock = AsyncMock()

    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = client_mock
    context_manager.__aexit__.return_value = None

    async def mock_get_client():
        return context_manager

    monkeypatch.setattr(
        "prefect_cloud.cli.github.install_github_app_interactively", AsyncMock()
    )
    monkeypatch.setattr(
        "prefect_cloud.cli.github.get_prefect_cloud_client", mock_get_client
    )

    return install_mock, client_mock


def test_github_setup(mock_outgoing_calls):
    """Test the GitHub setup command."""
    _, client_mock = mock_outgoing_calls

    test_repos = ["owner/repo1", "owner/repo2"]
    client_mock.get_github_repositories.return_value = test_repos

    invoke_and_assert(
        command=["github", "setup"],
        expected_code=0,
        expected_output_contains=[
            "✓ Prefect Cloud GitHub integration complete",
            "Connected repositories:",
            "- owner/repo1",
            "- owner/repo2",
        ],
    )


def test_github_setup_no_repositories(mock_outgoing_calls):
    """Test the GitHub setup command when no repositories are available."""
    _, client_mock = mock_outgoing_calls

    client_mock.get_github_repositories.return_value = []

    invoke_and_assert(
        command=["github", "setup"],
        expected_code=1,
        expected_output_contains=[
            "✗ No repositories found",
            "This may mean:",
            "• The integration was not successful, or",
            "• The integration is still pending GitHub admin approval",
            "Once approved, you’ll be able to deploy from your GitHub repos using:",
            "prefect-cloud deploy <file.py:function> --from <github repo>",
        ],
    )


def test_github_ls_with_repositories(mock_outgoing_calls):
    """Test the GitHub ls command when repositories are available."""
    _, client_mock = mock_outgoing_calls

    test_repos = ["owner/repo1", "owner/repo2", "owner/repo3"]
    client_mock.get_github_repositories.return_value = test_repos

    invoke_and_assert(
        command=["github", "ls"],
        expected_code=0,
        expected_output_contains=[
            "Connected repositories:",
            "- owner/repo1",
            "- owner/repo2",
            "- owner/repo3",
        ],
    )


def test_github_ls_no_repositories(mock_outgoing_calls):
    """Test the GitHub ls command when no repositories are available."""
    _, client_mock = mock_outgoing_calls

    client_mock.get_github_repositories.return_value = []

    invoke_and_assert(
        command=["github", "ls"],
        expected_code=1,
        expected_output_contains=[
            "✗ No repositories found",
            "This likely means:",
            "• The GitHub integration has not been set up, or",
            "• The integration is pending GitHub admin approval",
            "To get started:",
            "  Run: prefect-cloud github setup",
            "If the integration is pending:",
            "  Once a GitHub admin approves the installation, you can deploy from your repos",
            "  prefect-cloud deploy <file.py:function> --from <github repo>",
        ],
    )


def test_github_ls_exception_handling(mock_outgoing_calls):
    """Test the GitHub ls command when an exception occurs."""
    _, client_mock = mock_outgoing_calls

    client_mock.get_github_repositories.side_effect = Exception("Connection error")

    invoke_and_assert(
        command=["github", "ls"],
        expected_code=1,
        expected_output_contains="Connection error",
    )
