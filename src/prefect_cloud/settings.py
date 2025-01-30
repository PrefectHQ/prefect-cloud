from typing import ClassVar

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="PREFECT_CLOUD_", env_file=".env", extra="ignore"
    )

    default_managed_work_pool_name: str = Field(
        default="default-work-pool",
        description="Default name when creating a managed work pool.",
    )


settings = Settings()
