# prefect-cloud

:zap: Deploy your code on Prefect Cloud in seconds! :zap:

## Installation
All you need is `uv`! See [installation docs here](https://docs.astral.sh/uv/getting-started/installation/)
```bash
$ curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Login to Prefect Cloud
```bash
$ uvx prefect cloud login
```

## Grab your workflow
Pick a Python file with the function(s) you want to deploy (or use the example below). 
The function you specify will be automatically converted into a Prefect `@flow` if it isn't already. 

```python
$ cat << 'EOF' > example_workflow.py
def get_message():
    return "Hello, World!"

def hello_world():
    print(get_message())

def greet_user(name: str, exclaim: bool = False):
    message = f"Hello, {name}"
    if exclaim:
        message += "!"
    print(message)
EOF
```

## Deploy

### From local source:
Store your code with prefect cloud for easy deployment.
```bash
$ uvx prefect-cloud deploy example_workflow.py hello_world
```
With additional code files or directories:
```bash
# files
$ ... --include "file1.py" --include "/path/to/file2.py"
# directories
$ ... --include "/dir1/" --include "/path/to/dir2/"
```

### From github
```bash
$ uvx prefect-cloud git-deploy https://github.com/jakekaplan/demo-flows/blob/main/hello_world.py hello_world
```

### With dependencies:
```bash
# a package
$ ... --dependencies pandas
# multiple packages
$ ... --dependencies "pandas,numpy"
# requirements file
$ ... --dependencies /path/to/requirements.txt
# pyproject.toml
$ ... --dependencies /path/to/pyproject.toml
```

### With environment variables:
```bash
$ ... --env ENV_VAR1=VALUE1 --env ENV_VAR2=VALUE2
```
