# prefect-cloud

:zap: Deploy your code on Prefect Cloud in seconds! :zap:

Deploy and run your Python functions on Prefect Cloud with a single command.

## Installation
First, install `uv` if you haven't already. See [installation docs here](https://docs.astral.sh/uv/getting-started/installation/)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then install prefect-cloud:
```bash
uv pip install prefect-cloud
```

Alternatively, you can run `prefect-cloud` as a tool without installing it using `uvx`. See [uv tools guide](https://docs.astral.sh/uv/guides/tools/) for more details.

## Login to Prefect Cloud

```bash
prefect-cloud login
```

## Deploy your workflow

Deploy any Python function from a GitHub repository. For example:

```python
# https://github.com/PrefectHQ/prefect-cloud/blob/main/examples/hello.py

def hello_world():
    print("Hello, World!")
```

Deploy it using:
```bash
prefect-cloud deploy ... --from https://github.com/PrefectHQ/prefect-cloud/blob/main/examples/hello.py
```

### Options

**Deploy and Run**
```bash
prefect-cloud deploy ... --run --parameters name=value
```
Parameters must match the function's arguments. For example, if your function is `def hello(name: str)`, use `--parameters name=Alice`.

**Dependencies**

Specify dependencies using `--with`:
```bash
# Package names
prefect-cloud deploy ... --with pandas --with numpy

# Or from files
prefect-cloud deploy ... --with requirements.txt
prefect-cloud deploy ... --with pyproject.toml
```

**Environment Variables**
```bash
prefect-cloud deploy ... --env KEY=VALUE --env KEY2=VALUE2
```

**Private Repositories**
```bash
prefect-cloud deploy ... --from https://github.com/myorg/private-repo/blob/main/flows.py --credentials GITHUB_TOKEN
```

## Managing Deployments

List all deployments:
```bash
prefect-cloud ls
```

Run a deployment:
```bash
prefect-cloud run function_name/deployment_name
```

Schedule a deployment (using cron):
```bash
prefect-cloud schedule function_name/deployment_name "*/5 * * * *"  # Run every 5 minutes
prefect-cloud schedule function_name/deployment_name none  # Remove schedule
```
Format: `minute hour day-of-month month day-of-week`

Pause/Resume a deployment:
```bash
prefect-cloud pause function_name/deployment_name
prefect-cloud resume function_name/deployment_name
```
