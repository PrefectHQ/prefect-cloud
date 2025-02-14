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
# https://github.com/ExampleOwner/example-repo-cloud/blob/main/examples/hello.py

def hello_world():
    print("Hello, World!")
```

### Deploy to Prefect Cloud
```
prefect-cloud deploy <path/to/file.py:function_name> --from <source repo URL>
```
e.g.
```bash
prefect-cloud deploy examples/hello.py:hello_world --from https://github.com/PrefectHQ/prefect-cloud/
```

### Run it with
```bash
prefect-cloud run <flow_name>/<deployment_name>
````
e.g.
```bash
prefect-cloud run hello_world/hello_world
```

### Schedule it with
```bash
prefect-cloud schedule <flow_name>/<deployment_name> <SCHEDULE>
````
e.g.
```bash
prefect-cloud schedule hello_world/hello_world "0 * * * *"
```


### Additional Options

**Add Dependencies**
```bash
# Add dependencies
prefect-cloud deploy ... --with pandas --with numpy

# Or install from requirements file at runtime
prefect-cloud deploy ... --with-requirements </path/to/requirements.txt>
```

**Inclcude Environment Variables**
```bash
prefect-cloud deploy ... --env KEY=VALUE --env KEY2=VALUE2
```

**From a Private Repository**
```bash
prefect-cloud deploy ... https://github.com/myorg/private-repo/blob/main/flows.py --credentials GITHUB_TOKEN
```

