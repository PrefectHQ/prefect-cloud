import subprocess
from pathlib import Path
from typing_extensions import Self

import toml


class InvalidDependencies(Exception):
    pass


class Dependencies:
    def __init__(
        self, dependencies: list[str] | None = None, validate: bool = True
    ) -> None:
        self.dependencies = dependencies or []
        if self.dependencies and validate:
            self._validate(self.dependencies)

    def _validate(self, packages: list[str]) -> None:
        proc = subprocess.run(
            ["uv", "pip", "install", "--dry-run"] + [p.strip() for p in packages],
            capture_output=True,
            text=True,
        )

        if proc.returncode != 0:
            raise InvalidDependencies(proc.stderr)

    @classmethod
    def from_requirements_file(cls, filename: str) -> Self:
        path = Path(filename)
        dependencies: list[str] = []
        for line in path.read_text().splitlines():
            line = line.strip()

            if line and not line.startswith("#"):
                dependencies.append(line.split("#")[0].strip())

        return cls(dependencies)

    @classmethod
    def from_pyproject_toml(cls, filename: str) -> Self:
        pyproject_data = toml.load(filename)

        dependencies = pyproject_data.get("project", {}).get("dependencies", [])

        return cls(dependencies)

    @classmethod
    def from_comma_separated_string(cls, requirements: str) -> Self:
        dependencies = [
            dep.strip() for dep in requirements.replace(",", " ").split() if dep.strip()
        ]
        return cls(dependencies)


def get_dependencies(all_dependencies: list[str]) -> list[str]:
    extracted: list[str] = []

    for dependency in all_dependencies:
        path = Path(dependency)
        if path.exists():
            if path.name == "pyproject.toml":
                extracted.extend(
                    Dependencies.from_pyproject_toml(dependency).dependencies
                )
            else:
                extracted.extend(
                    Dependencies.from_requirements_file(dependency).dependencies
                )
        else:
            if "," in dependency:
                extracted.extend(
                    Dependencies.from_comma_separated_string(dependency).dependencies
                )
            else:
                extracted.append(dependency)

    return extracted
