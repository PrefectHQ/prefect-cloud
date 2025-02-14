from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from httpx import AsyncClient


class FileNotFound(Exception):
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
        """Parse a GitHub repo URL into its components.

        Handles various URL formats:
        - https://github.com/owner/repo
        - github.com/owner/repo/tree/branch
        - github.com/owner/repo/tree/a1b2c3d

        Args:
            url: GitHub URL to parse

        Returns:
            GitHubRepo

        Raises:
            ValueError: If URL format is invalid
        """
        normalized_url = url if "://" in url else f"https://{url}"

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

    def to_pull_step(self, credentials_block: str | None = None) -> dict[str, Any]:
        pull_step_kwargs = {
            "id": "git-clone",
            "repository": self.clone_url,
            "branch": self.ref,
        }
        if credentials_block:
            pull_step_kwargs["access_token"] = (
                "{{ prefect.blocks.secret." + credentials_block + " }}"
            )

        return {"prefect.deployments.steps.git_clone": pull_step_kwargs}
