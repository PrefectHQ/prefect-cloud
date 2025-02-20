import os
import subprocess
import tempfile
from pathlib import Path

import click.exceptions
import pytest
from httpx import Response

from prefect_cloud.github import (
    FileNotFound,
    GitHubRepo,
    get_local_repo_urls,
    infer_repo_url,
)


class TestGitHubRepo:
    def test_from_url_basic(self):
        """Test basic repo URL without branch."""
        url = "https://github.com/ExampleOwner/example-repo"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "ExampleOwner"
        assert ref.repo == "example-repo"
        assert ref.ref == "main"  # Default branch

    def test_from_url_with_branch(self):
        """Test repo URL with specific branch."""
        url = "github.com/ExampleOwner/example-repo/tree/dev"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "ExampleOwner"
        assert ref.repo == "example-repo"
        assert ref.ref == "dev"

    def test_from_url_with_commit(self):
        """Test repo URL with commit SHA."""
        url = "github.com/ExampleOwner/example-repo/tree/a1b2c3d4e5f6"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "ExampleOwner"
        assert ref.repo == "example-repo"
        assert ref.ref == "a1b2c3d4e5f6"

    def test_from_url_with_git_extension(self):
        """Test repo URL with .git extension."""
        url = "github.com/ExampleOwner/example-repo.git"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "ExampleOwner"
        assert ref.repo == "example-repo"  # .git is stripped
        assert ref.ref == "main"

    def test_from_url_without_protocol(self):
        """Test URL without https://."""
        url = "github.com/ExampleOwner/example-repo"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "ExampleOwner"
        assert ref.repo == "example-repo"
        assert ref.ref == "main"

    def test_from_url_with_http(self):
        """Test URL with http:// instead of https://."""
        url = "http://github.com/ExampleOwner/example-repo"
        ref = GitHubRepo.from_url(url)

        assert ref.owner == "ExampleOwner"
        assert ref.repo == "example-repo"
        assert ref.ref == "main"

    def test_from_url_invalid_github(self):
        """Test that non-GitHub URLs are rejected."""
        with pytest.raises(ValueError, match="Not a GitHub URL"):
            GitHubRepo.from_url("https://gitlab.com/owner/repo")

    def test_from_url_invalid_format(self):
        """Test that URLs without owner/repo are rejected."""
        with pytest.raises(ValueError, match="Must include owner and repository"):
            GitHubRepo.from_url("https://github.com/owner")

    def test_from_url_rejects_file_urls(self):
        """Test that GitHub file URLs are rejected."""
        file_urls = [
            "https://github.com/ExampleOwner/example-repo/blob/main/README.md",
            "github.com/ExampleOwner/example-repo/blob/main/src/prefect/__init__.py",
            "https://github.com/ExampleOwner/example-repo/raw/main/requirements.txt",
        ]

        for url in file_urls:
            with pytest.raises(
                ValueError, match="URL appears to point to a specific file"
            ):
                GitHubRepo.from_url(url)

    def test_clone_url(self):
        """Test generation of clone URL."""
        ref = GitHubRepo(
            owner="ExampleOwner",
            repo="example-repo",
            ref="main",
        )
        assert ref.clone_url == "https://github.com/ExampleOwner/example-repo.git"

    def test_str_representation(self):
        """Test string representation of repo reference."""
        ref = GitHubRepo(
            owner="ExampleOwner",
            repo="example-repo",
            ref="main",
        )
        assert str(ref) == "github.com/ExampleOwner/example-repo @ main"


class TestGitHubContent:
    @pytest.mark.asyncio
    async def test_get_file_contents(self, respx_mock):
        """Test getting file contents from repo."""
        github_ref = GitHubRepo(
            owner="ExampleOwner",
            repo="example-repo",
            ref="main",
        )

        expected_content = "# Test Content"
        api_url = "https://api.github.com/repos/ExampleOwner/example-repo/contents/README.md?ref=main"
        respx_mock.get(api_url).mock(
            return_value=Response(status_code=200, text=expected_content)
        )

        content = await github_ref.get_file_contents("README.md")
        assert content == expected_content

    @pytest.mark.asyncio
    async def test_get_file_contents_with_credentials(self, respx_mock):
        """Test getting file contents with authentication."""
        github_ref = GitHubRepo(
            owner="ExampleOwner",
            repo="example-repo",
            ref="main",
        )

        test_token = "test-token"
        expected_content = "# Test Content"
        api_url = "https://api.github.com/repos/ExampleOwner/example-repo/contents/README.md?ref=main"

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
            owner="ExampleOwner",
            repo="example-repo",
            ref="main",
        )

        api_url = "https://api.github.com/repos/ExampleOwner/example-repo/contents/NONEXISTENT.md?ref=main"
        respx_mock.get(api_url).mock(return_value=Response(status_code=404))

        with pytest.raises(FileNotFound, match="File not found: NONEXISTENT.md in"):
            await github_ref.get_file_contents("NONEXISTENT.md")

    def test_to_pull_step(self):
        """Test generation of pull step configuration."""
        github_ref = GitHubRepo(
            owner="ExampleOwner",
            repo="example-repo",
            ref="main",
        )

        pull_step = github_ref.to_pull_step()
        assert pull_step == {
            "prefect.deployments.steps.git_clone": {
                "id": "git-clone",
                "repository": "https://github.com/ExampleOwner/example-repo.git",
                "branch": "main",
            }
        }

    def test_to_pull_step_with_credentials(self):
        """Test generation of pull step with credentials."""
        github_ref = GitHubRepo(
            owner="ExampleOwner",
            repo="example-repo",
            ref="main",
        )

        pull_step = github_ref.to_pull_step(credentials_block="test-creds")
        assert pull_step == {
            "prefect.deployments.steps.git_clone": {
                "id": "git-clone",
                "repository": "https://github.com/ExampleOwner/example-repo.git",
                "branch": "main",
                "access_token": "{{ prefect.blocks.secret.test-creds }}",
            }
        }


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    """Create a temporary git repository."""
    os.chdir(tmp_path)
    subprocess.run(["git", "init"], check=True, capture_output=True)
    return tmp_path


class TestInferRepoUrl:
    def test_infers_https_url(self, git_repo: Path):
        subprocess.run(
            [
                "git",
                "remote",
                "add",
                "origin",
                "https://github.com/ExampleOwner/example-repo",
            ],
            check=True,
            capture_output=True,
        )

        assert infer_repo_url() == "https://github.com/ExampleOwner/example-repo"

    def test_infers_ssh_url(self, git_repo: Path):
        subprocess.run(
            [
                "git",
                "remote",
                "add",
                "origin",
                "git@github.com:ExampleOwner/example-repo.git",
            ],
            check=True,
            capture_output=True,
        )

        assert infer_repo_url() == "https://github.com/ExampleOwner/example-repo"

    def test_exits_when_not_git_repo(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)

            with pytest.raises(click.exceptions.Exit):
                infer_repo_url()

    def test_exits_when_not_github_url(self, git_repo: Path):
        subprocess.run(
            [
                "git",
                "remote",
                "add",
                "origin",
                "https://gitlab.com/ExampleOwner/example-repo",
            ],
            check=True,
            capture_output=True,
        )

        with pytest.raises(click.exceptions.Exit):
            infer_repo_url()


class TestGetLocalRepoUrls:
    def test_returns_empty_list_when_not_git_repo(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)

            assert get_local_repo_urls() == []

    def test_returns_github_urls(self, git_repo: Path):
        remotes = [
            ("origin", "https://github.com/ExampleOwner/example-repo"),
            ("upstream", "https://github.com/UpstreamOwner/example-repo"),
        ]

        for name, url in remotes:
            subprocess.run(
                ["git", "remote", "add", name, url],
                check=True,
                capture_output=True,
            )

        urls = get_local_repo_urls()
        assert len(urls) == 2
        assert set(urls) == {remote[1] for remote in remotes}

    def test_filters_non_github_urls(self, git_repo: Path):
        remotes = [
            ("origin", "https://github.com/ExampleOwner/example-repo"),
            ("gitlab", "https://gitlab.com/ExampleOwner/example-repo"),
            ("upstream", "https://github.com/UpstreamOwner/example-repo"),
        ]

        for name, url in remotes:
            subprocess.run(
                ["git", "remote", "add", name, url],
                check=True,
                capture_output=True,
            )

        urls = get_local_repo_urls()
        assert len(urls) == 2
        assert set(urls) == {
            "https://github.com/ExampleOwner/example-repo",
            "https://github.com/UpstreamOwner/example-repo",
        }

    def test_translates_ssh_urls(self, git_repo: Path):
        remotes = [
            ("origin", "git@github.com:ExampleOwner/example-repo.git"),
            ("upstream", "https://github.com/UpstreamOwner/example-repo"),
        ]

        for name, url in remotes:
            subprocess.run(
                ["git", "remote", "add", name, url],
                check=True,
                capture_output=True,
            )

        urls = get_local_repo_urls()
        assert len(urls) == 2
        assert set(urls) == {
            "https://github.com/ExampleOwner/example-repo",
            "https://github.com/UpstreamOwner/example-repo",
        }
