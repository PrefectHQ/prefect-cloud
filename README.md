# prefect-cloud

:zap: Deploy your code to Prefect Cloud in seconds! :zap:

Deploy and run your Python functions on Prefect Cloud with a single command.

## Installation
First, install `uv` if you haven't already. See [installation docs here](https://docs.astral.sh/uv/getting-started/installation/)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Create and activate a virtual environment:
```bash
uv venv && source .venv/bin/activate
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

Deploy and run it using:
```
prefect-cloud deploy <path/to/file.py:function_name> --from <source repo URL> --run
```
e.g.
```bash
prefect-cloud deploy examples/hello.py:hello_world --from https://github.com/PrefectHQ/prefect-cloud/ --run
```

### Options
**Only Deploy**
```bash
prefect-cloud deploy ... --from ...
```

**Deploy and Run**
```bash
prefect-cloud deploy ... --from ... --run --parameter a=1 --parameter b=2 
```

**Dependencies**

```bash
# Add dependencies
prefect-cloud deploy ... --from ... --with pandas --with numpy

# Or install from requirements file at runtime
prefect-cloud deploy ... --from ... --with-requirements </path/to/requirements.txt>
```

**Environment Variables**
```bash
prefect-cloud deploy ... --from ... --env KEY=VALUE --env KEY2=VALUE2
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
