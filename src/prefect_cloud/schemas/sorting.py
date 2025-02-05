from prefect_cloud.utilities.collections import AutoEnum


class DeploymentSort(AutoEnum):
    """Defines deployment sorting options."""

    CREATED_DESC = AutoEnum.auto()
    UPDATED_DESC = AutoEnum.auto()
    NAME_ASC = AutoEnum.auto()
    NAME_DESC = AutoEnum.auto()
    CONCURRENCY_LIMIT_ASC = AutoEnum.auto()
    CONCURRENCY_LIMIT_DESC = AutoEnum.auto()
