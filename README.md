# prefect-cloud

:zap: Deploy your code on Prefect Cloud in seconds! :zap:

## Installation
All you need is `uv`! See [installation docs here](https://docs.astral.sh/uv/getting-started/installation/)
```bash
$ curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Login to Prefect Cloud
```bash
$ uvx prefect-cloud login
```

## Deploy your workflow from github

```shell
$ uvx prefect-cloud deploy FUNCTION_NAME --from GITHUB_PY_FILE_URL
```
For example:
```shell
$ uvx prefect-cloud deploy hello_world --from https://github.com/jakekaplan/demo-flows/blob/main/hello_world.py
```
### From a Private Repo
```shell
# private repo
$ uvx prefect-cloud deploy FUNCTION_NAME --from GITHUB_PY_FILE_URL --credentials GITHUB_TOKEN
```

### With dependencies:
```bash
# a package
$ uvx prefect-cloud deploy ... --from ... --dependencies pandas
# multiple packages
$ uvx prefect-cloud deploy ... --from ... --dependencies "pandas,numpy"
# requirements file
$ uvx prefect-cloud deploy ... --from ... --dependencies /path/to/requirements.txt
# pyproject.toml
$ uvx prefect-cloud deploy ... --from ... --dependencies /path/to/pyproject.toml
```

### With environment variables:
```bash
$ uvx prefect-cloud deploy ... --from ... --env ENV_VAR1=VALUE1 --env ENV_VAR2=VALUE2
```
