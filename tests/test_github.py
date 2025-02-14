import pytest
from httpx import Response

from prefect_cloud.github import FileNotFound, GitHubRepo


class TestGitHubRepo:
    def test_from_url_basic(self):
        """Test basic repo URL without branch."""
        url = "https://github.com/PrefectHQ/prefect"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"  # Default branch

    def test_from_url_with_branch(self):
        """Test repo URL with specific branch."""
        url = "github.com/PrefectHQ/prefect/tree/dev"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "dev"

    def test_from_url_with_commit(self):
        """Test repo URL with commit SHA."""
        url = "github.com/PrefectHQ/prefect/tree/a1b2c3d4e5f6"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "a1b2c3d4e5f6"

    def test_from_url_with_git_extension(self):
        """Test repo URL with .git extension."""
        url = "github.com/PrefectHQ/prefect.git"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"  # .git is stripped
        assert ref.ref == "main"

    def test_from_url_without_protocol(self):
        """Test URL without https://."""
        url = "github.com/PrefectHQ/prefect"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"

    def test_from_url_with_http(self):
        """Test URL with http:// instead of https://."""
        url = "http://github.com/PrefectHQ/prefect"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "PrefectHQ"
        assert ref.repo == "prefect"
        assert ref.ref == "main"

    def test_from_url_invalid_github(self):
        """Test that non-GitHub URLs are rejected."""
        with pytest.raises(ValueError, match="Not a GitHub URL"):
            GitHubRepo.from_url("https://gitlab.com/owner/repo")

    def test_from_url_invalid_format(self):
        """Test that URLs without owner/repo are rejected."""
        with pytest.raises(ValueError, match="Must include owner and repository"):
            GitHubRepo.from_url("https://github.com/owner")

    def test_clone_url(self):
        """Test generation of clone URL."""
        ref = GitHubRepo(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
        )
        assert ref.clone_url == "https://github.com/PrefectHQ/prefect.git"

    def test_str_representation(self):
        """Test string representation of repo reference."""
        ref = GitHubRepo(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
        )
        assert str(ref) == "github.com/PrefectHQ/prefect @ main"


class TestGitHubContent:
    @pytest.mark.asyncio
    async def test_get_file_contents(self, respx_mock):
        """Test getting file contents from repo."""
        github_ref = GitHubRepo(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
        )

        expected_content = "# Test Content"
        api_url = (
            "https://api.github.com/repos/PrefectHQ/prefect/contents/README.md?ref=main"
        )
        respx_mock.get(api_url).mock(
            return_value=Response(status_code=200, text=expected_content)
        )

        content = await github_ref.get_file_contents("README.md")
        assert content == expected_content

    @pytest.mark.asyncio
    async def test_get_file_contents_with_credentials(self, respx_mock):
        """Test getting file contents with authentication."""
        github_ref = GitHubRepo(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
        )

        test_token = "test-token"
        expected_content = "# Test Content"
        api_url = (
            "https://api.github.com/repos/PrefectHQ/prefect/contents/README.md?ref=main"
        )

        mock = respx_mock.get(api_url).mock(
            return_value=Response(status_code=200, text=expected_content)
        )

        content = await github_ref.get_file_contents(
            "README.md", credentials=test_token
        )
        assert content == expected_content

        # Verify authorization header was sent
        assert mock.calls[0].request.headers["Authorization"] == f"Bearer {test_token}"
        assert (
            mock.calls[0].request.headers["Accept"] == "application/vnd.github.v3.raw"
        )

    @pytest.mark.asyncio
    async def test_get_file_contents_not_found(self, respx_mock):
        """Test handling of non-existent files."""
        github_ref = GitHubRepo(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
        )

        api_url = "https://api.github.com/repos/PrefectHQ/prefect/contents/NONEXISTENT.md?ref=main"
        respx_mock.get(api_url).mock(return_value=Response(status_code=404))

        with pytest.raises(FileNotFound, match="File not found: NONEXISTENT.md in"):
            await github_ref.get_file_contents("NONEXISTENT.md")

    def test_to_pull_step(self):
        """Test generation of pull step configuration."""
        github_ref = GitHubRepo(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
        )

        pull_step = github_ref.to_pull_step()
        assert pull_step == {
            "prefect.deployments.steps.git_clone": {
                "repository": "https://github.com/PrefectHQ/prefect.git",
                "branch": "main",
            }
        }

    def test_to_pull_step_with_credentials(self):
        """Test generation of pull step with credentials."""
        github_ref = GitHubRepo(
            owner="PrefectHQ",
            repo="prefect",
            ref="main",
        )

        pull_step = github_ref.to_pull_step(credentials_block="test-creds")
        assert pull_step == {
            "prefect.deployments.steps.git_clone": {
                "repository": "https://github.com/PrefectHQ/prefect.git",
                "branch": "main",
                "access_token": "{{ prefect.blocks.secret.test-creds }}",
            }
        }
