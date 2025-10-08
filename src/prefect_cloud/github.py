import os
import subprocess
import threading
import webbrowser
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlparse

from httpx import AsyncClient

from prefect_cloud.auth import CLOUD_UI_URL, get_account_id
from prefect_cloud.client import PrefectCloudClient
from prefect_cloud.utilities.callback import callback_server


class FileNotFound(Exception):
    pass


class RepoUnknown(Exception):
    pass


@dataclass
class GitHubRepo:
    """Reference to a GitHub repository."""

    owner: str
    repo: str
    ref: str  # Can be either a branch name or commit SHA

    @property
    def clone_url(self) -> str:
        """Get the HTTPS URL for cloning this repository."""
        return f"https://github.com/{self.owner}/{self.repo}.git"

    def __str__(self) -> str:
        return f"github.com/{self.owner}/{self.repo} @ {self.ref}"

    @classmethod
    def from_url(cls, url: str) -> "GitHubRepo":
        """Parse a GitHub repo URL or owner/repo string into its components.

        Handles various URL formats:
        - https://github.com/owner/repo
        - github.com/owner/repo/tree/branch
        - github.com/owner/repo/tree/a1b2c3d
        - owner/repo
        - owner/repo/tree/branch

        Args:
            url: GitHub URL or owner/repo string to parse

        Returns:
            GitHubRepo

        Raises:
            ValueError: If URL format is invalid
        """
        # Special case for simple owner/repo format
        if "/" in url and not any(
            x in url for x in ["://", "github.com", "tree", "blob", "raw"]
        ):
            parts = url.split("/")
            if len(parts) == 2:
                owner, repo = parts
                return cls(owner=owner, repo=repo.removesuffix(".git"), ref="main")
            elif len(parts) >= 4 and parts[2] == "tree":
                owner, repo = parts[0:2]
                ref = parts[3]
                return cls(owner=owner, repo=repo.removesuffix(".git"), ref=ref)

        # Handle URL formats as before
        normalized_url = url
        if "://" not in url:
            if not url.startswith("github.com"):
                normalized_url = f"https://github.com/{url}"
            else:
                normalized_url = f"https://{url}"

        # Check for GitHub file URLs (blob/raw paths)
        if "/blob/" in normalized_url or "/raw/" in normalized_url:
            raise ValueError(
                "Invalid GitHub URL: URL appears to point to a specific file. "
                "Must be a repository URL (e.g., github.com/owner/repo)"
            )

        parsed = urlparse(normalized_url)
        if parsed.netloc != "github.com":
            raise ValueError("Not a GitHub URL. Must include 'github.com' in the URL")

        path_parts = [p for p in parsed.path.split("/") if p]
        if len(path_parts) < 2:
            raise ValueError("Invalid GitHub URL. Must include owner and repository")

        owner, repo, *rest = path_parts
        repo = repo.removesuffix(".git")
        # Extract ref from /tree/branch format if present
        ref = "main"
        if len(rest) >= 2 and rest[0] == "tree":
            ref = rest[1]

        return cls(owner=owner, repo=repo, ref=ref)

    async def get_file_contents(
        self, filepath: str, credentials: str | None = None
    ) -> str:
        """Get the contents of a file from this repository.

        Args:
            filepath: Path to the file in the repository
            credentials: Optional GitHub credentials for private repos

        Returns:
            The contents of the file as a string

        Raises:
            FileNotFound: If the file doesn't exist
            ValueError: If the file can't be accessed
        """
        api_url = f"https://api.github.com/repos/{self.owner}/{self.repo}/contents/{filepath}?ref={self.ref}"
        headers: dict[str, str] = {
            "Accept": "application/vnd.github.v3.raw",
        }
        if credentials:
            headers["Authorization"] = f"Bearer {credentials}"

        async with AsyncClient() as client:
            response = await client.get(api_url, headers=headers)
            if response.status_code == 404:
                raise FileNotFound(f"File not found: {filepath} in {self}")
            response.raise_for_status()
            return response.text

    def public_repo_pull_steps(self) -> list[dict[str, Any]]:
        return [
            {
                "prefect.deployments.steps.git_clone": {
                    "id": "git-clone",
                    "repository": self.clone_url,
                    "branch": self.ref,
                }
            }
        ]

    def private_repo_via_block_pull_steps(
        self, credentials_block: str
    ) -> list[dict[str, Any]]:
        return [
            {
                "prefect.deployments.steps.git_clone": {
                    "id": "git-clone",
                    "repository": self.clone_url,
                    "branch": self.ref,
                    "access_token": "{{ prefect.blocks.secret."
                    + credentials_block
                    + " }}",
                }
            }
        ]

    def private_repo_via_github_app_pull_steps(self) -> list[dict[str, Any]]:
        clone_url = self.clone_url.replace(
            "https://", "https://x-access-token:{{ get-github-token.stdout }}@"
        )
        return [
            # Get GitHub App installation token
            {
                "prefect.deployments.steps.run_shell_script": {
                    "id": "get-github-token",
                    # The prefect image currently doesn't get built with uvx
                    # installed. So we use the long name instead of the alias.
                    "script": f"uv tool run prefect-cloud github token {self.owner}/{self.repo}",
                }
            },
            # Clone Step
            {
                "prefect.deployments.steps.git_clone": {
                    "id": "git-clone",
                    "repository": clone_url,
                    "branch": self.ref,
                }
            },
        ]


def translate_to_http(url: str) -> str:
    """
    Translate a git URL to an HTTP URL.
    """
    url = url.strip()

    if url.startswith("git@github.com:"):
        url = "https://github.com/" + url.split("git@github.com:")[1]

    if url.endswith(".git"):
        url = url.removesuffix(".git")

    return url


def infer_repo_url() -> str:
    """
    Infer the repository URL from the current directory.
    """
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            check=True,
        )
        url = result.stdout.strip()

        url = translate_to_http(url)

        if not url.startswith("https://github.com"):
            raise ValueError("Repository URL must be from github.com")

        return url

    except (subprocess.CalledProcessError, ValueError):
        raise RepoUnknown(
            "No repository specified, and this directory doesn't appear to be a "
            "GitHub repository.  Specify --from to indicate where Prefect Cloud will "
            "download your code from."
        )


def get_local_repo_urls() -> list[str]:
    """
    Get all local repository URLs from the current directory.
    """
    try:
        remotes = subprocess.run(
            ["git", "remote", "show"],
            capture_output=True,
            text=True,
            check=True,
        )
        all_urls: list[str] = []
        for remote in remotes.stdout.splitlines():
            result = subprocess.run(
                ["git", "remote", "get-url", remote],
                capture_output=True,
                text=True,
                check=True,
            )
            urls = [translate_to_http(url) for url in result.stdout.splitlines()]
            all_urls += [url for url in urls if url.startswith("https://github.com")]
        return all_urls
    except (subprocess.CalledProcessError, ValueError):
        return []


def _get_prefect_cloud_github_app() -> str:
    """Get the GitHub app name based on environment"""

    env = os.environ.get("CLOUD_ENV")

    if env in ("prd", "prod", None):
        github_app_name = "prefect-cloud"
    elif env == "stg":
        github_app_name = "prefect-cloud-stg"
    elif env == "dev":
        github_app_name = "prefect-cloud-dev"
    elif env == "lcl":
        github_app_name = "prefect-cloud-lcl"
    else:
        raise ValueError(f"Invalid CLOUD_ENV: {env}")

    return github_app_name


async def install_github_app_interactively(client: PrefectCloudClient) -> None:
    """Interactively install the Prefect Cloud GitHub app."""

    with callback_server() as callback_ctx:
        account_id = await get_account_id()
        github_app_name = _get_prefect_cloud_github_app()

        cloud_callback_url = f"{CLOUD_UI_URL}/account/{account_id}/integrations/success?callback={quote(callback_ctx.url)}&type=github"

        state_token = await client.get_github_state_token(
            redirect_url=cloud_callback_url
        )
        install_app_url = f"https://github.com/apps/{github_app_name}/installations/new?state={state_token}"

        threading.Thread(
            target=webbrowser.open_new_tab, args=(install_app_url,), daemon=True
        ).start()

        callback_ctx.wait_for_callback()
