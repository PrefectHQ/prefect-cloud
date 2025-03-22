import textwrap
from unittest.mock import AsyncMock, patch

from prefect_cloud.github import FileNotFound
from prefect_cloud.schemas.objects import WorkPool
from tests.test_cli.utils import invoke_and_assert


def test_project_deploy_from_pyproject():
    """Test project deployment from pyproject.toml configuration"""
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
        client.get_github_token = AsyncMock(return_value=None)

        # Mock auth.get_cloud_urls_or_login
        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            # Mock GitHub content retrieval for pyproject.toml
            with patch(
                "prefect_cloud.github.GitHubRepo.get_file_contents"
            ) as mock_content:
                # Define mock responses for different files
                def get_mock_content(path, *args, **kwargs):
                    if path == "pyproject.toml":
                        return textwrap.dedent("""
                            [tool.prefect-cloud]
                            source_repo = "github.com/owner/repo"
                            
                            [tool.prefect-cloud.groups.data-processors]
                            dependencies = ["pandas", "numpy"]
                            schedule = "0 * * * *"
                            
                            [[tool.prefect-cloud.deployments]]
                            group = "data-processors"
                            files = [
                              "flows/process_csv.py:process",
                              "flows/process_json.py:process"
                            ]
                        """).lstrip()
                    elif (
                        path == "flows/process_csv.py"
                        or path == "flows/process_json.py"
                    ):
                        return textwrap.dedent("""
                            def process():
                                pass
                        """).lstrip()
                    else:
                        raise FileNotFound(f"File not found: {path}")

                mock_content.side_effect = get_mock_content

                # Mock pull steps
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

                    invoke_and_assert(
                        command=[
                            "project",
                            "deploy",
                            "--from",
                            "github.com/owner/repo",
                        ],
                        expected_code=0,
                        expected_output_contains=[
                            "Deployed process",
                            "flows/process_csv.py:process",
                            "flows/process_json.py:process",
                            "Successfully deployed",
                        ],
                    )

                    # Verify we created 2 deployments (one for each file in the pyproject.toml)
                    assert client.create_managed_deployment.call_count == 2


def test_project_deploy_from_inline_metadata():
    """Test project deployment from PEP 723 inline script metadata"""
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
        client.get_github_token = AsyncMock(return_value=None)

        # Mock auth.get_cloud_urls_or_login
        with patch("prefect_cloud.cli.root.auth.get_cloud_urls_or_login") as mock_urls:
            mock_urls.return_value = ("https://ui.url", "https://api.url", "test-key")

            # Mock Path.glob to return our test file
            with patch("pathlib.Path.glob") as mock_glob:
                mock_glob.return_value = ["flows/process.py"]

                # Mock Path.is_dir
                with patch("pathlib.Path.is_dir") as mock_is_dir:
                    mock_is_dir.return_value = True

                    # Mock GitHub content retrieval
                    with patch(
                        "prefect_cloud.github.GitHubRepo.get_file_contents"
                    ) as mock_content:
                        # Mock pyproject.toml doesn't exist
                        def get_mock_content(path, *args, **kwargs):
                            if path == "pyproject.toml":
                                raise FileNotFound("File not found: pyproject.toml")
                            elif path == "flows/process.py":
                                return textwrap.dedent("""
                                    # /// script
                                    # requires-python = ">=3.11"
                                    # dependencies = ["pandas", "requests<3"]
                                    # [tool.prefect-cloud]
                                    # name = "data-processor"
                                    # schedule = "0 * * * *"
                                    # ///

                                    def process():
                                        # Function code
                                        pass
                                """).lstrip()
                            else:
                                raise FileNotFound(f"File not found: {path}")

                        mock_content.side_effect = get_mock_content

                        # Mock pull steps
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

                            invoke_and_assert(
                                command=[
                                    "project",
                                    "deploy",
                                    "--from",
                                    "github.com/owner/repo",
                                ],
                                expected_code=0,
                                expected_output_contains=[
                                    "Deployed data-processor",
                                    "flows/process.py:process",
                                    "Successfully deployed",
                                ],
                            )

                            # Verify one deployment was created
                            client.create_managed_deployment.assert_called_once()

                            # Verify the deployment was created with the correct parameters
                            call_kwargs = client.create_managed_deployment.call_args[1]
                            assert call_kwargs["deployment_name"] == "data-processor"
                            assert call_kwargs["filepath"] == "flows/process.py"
                            assert call_kwargs["function"] == "process"


def test_project_deploy_with_schedule():
    """Test project deployment with schedule parameter is properly passed to deployment scheduling"""
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
        client.get_github_token = AsyncMock(return_value=None)

        # Mock deployments.schedule function
        with patch("prefect_cloud.cli.root.deployments.schedule") as mock_schedule:
            mock_schedule.return_value = None

            # Mock auth.get_cloud_urls_or_login
            with patch(
                "prefect_cloud.cli.root.auth.get_cloud_urls_or_login"
            ) as mock_urls:
                mock_urls.return_value = (
                    "https://ui.url",
                    "https://api.url",
                    "test-key",
                )

                # Mock GitHub content retrieval
                with patch(
                    "prefect_cloud.github.GitHubRepo.get_file_contents"
                ) as mock_content:
                    # Define inline script metadata with a schedule
                    def get_mock_content(path, *args, **kwargs):
                        if path == "pyproject.toml":
                            raise FileNotFound("File not found: pyproject.toml")
                        elif path == "flows/process.py":
                            return textwrap.dedent("""
                                # /// script
                                # requires-python = ">=3.11"
                                # dependencies = ["pandas", "requests<3"]
                                # [tool.prefect-cloud]
                                # name = "data-processor"
                                # schedule = "0 * * * *"
                                # ///

                                def process():
                                    # Function code
                                    pass
                            """).lstrip()
                        else:
                            raise FileNotFound(f"File not found: {path}")

                    mock_content.side_effect = get_mock_content

                    # Mock Path operations
                    with patch("pathlib.Path.glob") as mock_glob:
                        mock_glob.return_value = ["flows/process.py"]

                        with patch("pathlib.Path.is_dir") as mock_is_dir:
                            mock_is_dir.return_value = True

                            # Mock pull steps
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

                                invoke_and_assert(
                                    command=[
                                        "project",
                                        "deploy",
                                        "--from",
                                        "github.com/owner/repo",
                                    ],
                                    expected_code=0,
                                    expected_output_contains=[
                                        "Deployed data-processor",
                                        "flows/process.py:process",
                                    ],
                                )

                                # Verify the schedule function was called with the correct parameters
                                mock_schedule.assert_called_once_with(
                                    "process/data-processor", "0 * * * *"
                                )
