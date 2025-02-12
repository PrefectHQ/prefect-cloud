from typing import Any

import pytest

from prefect_cloud.github import GitHubFileRef, InvalidGitHubURL


@pytest.mark.parametrize(
    "url, expected",
    [
        (
            "https://github.com/prefecthq/prefect/blob/main/flows/hello.py",
            {
                "owner": "prefecthq",
                "repo": "prefect",
                "branch": "main",
                "filepath": "flows/hello.py",
            },
        ),
        (
            "gh/prefecthq/prefect@dev/flows/hello.py",
            {
                "owner": "prefecthq",
                "repo": "prefect",
                "branch": "dev",
                "filepath": "flows/hello.py",
            },
        ),
        (
            "gh/prefecthq/prefect/flows/hello.py",  # defaults to main branch
            {
                "owner": "prefecthq",
                "repo": "prefect",
                "branch": "main",
                "filepath": "flows/hello.py",
            },
        ),
    ],
)
def test_valid_github_url(url: str, expected: dict[str, Any]):
    github_ref = GitHubFileRef.from_url(url)
    for key, value in expected.items():
        assert getattr(github_ref, key) == value


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/org_slash_repo/hello.py",
        "geethahb/prefecthq/prefect/flows/hello.py",
    ],
)
def test_invalid_github_url(url: str):
    with pytest.raises(InvalidGitHubURL, match="Invalid GitHub URL"):
        GitHubFileRef.from_url(url)
