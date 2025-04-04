# pyright: reportUnusedImport=false

# Import CLI submodules to register them to the app
# isort: split


from . import auth  # noqa: F401
from . import deployments  # noqa: F401
from . import github  # noqa: F401
