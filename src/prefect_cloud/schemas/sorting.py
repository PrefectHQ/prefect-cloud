from prefect_cloud.utilities.collections import AutoEnum


class DeploymentSort(AutoEnum):
    """Defines deployment sorting options."""

    CREATED_DESC = AutoEnum.auto()
    UPDATED_DESC = AutoEnum.auto()
    NAME_ASC = AutoEnum.auto()
    NAME_DESC = AutoEnum.auto()
    CONCURRENCY_LIMIT_ASC = AutoEnum.auto()
    CONCURRENCY_LIMIT_DESC = AutoEnum.auto()


class FlowRunSort(AutoEnum):
    """Defines flow run sorting options."""

    ID_DESC = AutoEnum.auto()
    START_TIME_ASC = AutoEnum.auto()
    START_TIME_DESC = AutoEnum.auto()
    EXPECTED_START_TIME_ASC = AutoEnum.auto()
    EXPECTED_START_TIME_DESC = AutoEnum.auto()
    NAME_ASC = AutoEnum.auto()
    NAME_DESC = AutoEnum.auto()
    NEXT_SCHEDULED_START_TIME_ASC = AutoEnum.auto()
    END_TIME_DESC = AutoEnum.auto()
