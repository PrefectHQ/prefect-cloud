# pyright: reportUnusedImport=false

import prefect_cloud.cli.root

# Import CLI submodules to register them to the app
# isort: split

import prefect_cloud.cli.github  # noqa: F401
