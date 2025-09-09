CLAUDE.md
This file provides guidance to Claude Code when working with this repository.

Project Overview
Prefect Cloud is a package to enable onboarding to Prefect Cloud and works alongside the prefect PyPI package. 

Services:



Essential Commands

Development Guidelines
Code Conventions
Python 3.12 with modern typing syntax
Avoid # type: ignore, think harder about types
Follow surrounding code style
Minimal approaches before complex solutions
Don't use inline Python imports, except to resolve circular dependencies

Testing
Test directory structure mirrors src/ structure (e.g., tests/cli/, tests/utilities/)


Project Practices
Use Linear for issue tracking (ENG-/PLA- prefixes), not GitHub issues
NEVER commit with --no-verify - pre-commit hooks are required
Run pre-commit run --all-files to validate changes
Dependency updates: modify requirements.in or requirements-dev.in, then make
PR closing Linear issues: mention "Closes ENG-1234" in PR body
CLAUDE.md is always symlinked to AGENTS.md
PR description should be brief and focused on the problem being solved
PR descriptions should not include "Test Plan" checklists