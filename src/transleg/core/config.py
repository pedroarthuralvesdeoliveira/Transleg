from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    portal_base_url: str = Field(
        default="https://2242.aleff.com.br/",
        validation_alias=AliasChoices("TRANSLEG_PORTAL_BASE_URL"),
    )
    carrier_code: str | None = Field(
        default=None,
        validation_alias=AliasChoices("TRANSLEG_CARRIER_CODE", "CARRIER_CODE"),
    )
    portal_username: str | None = Field(
        default=None,
        validation_alias=AliasChoices("TRANSLEG_PORTAL_USERNAME", "CPF"),
    )
    portal_password: str | None = Field(
        default=None,
        validation_alias=AliasChoices("TRANSLEG_PORTAL_PASSWORD", "PASSWORD"),
    )
    database_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("TRANSLEG_DATABASE_URL", "DATABASE_URL"),
    )
    browser_headless: bool = Field(
        default=True,
        validation_alias=AliasChoices("TRANSLEG_BROWSER_HEADLESS", "BROWSER_HEADLESS"),
    )
    browser_download_dir: Path = Field(
        default=Path("./downloads"),
        validation_alias=AliasChoices(
            "TRANSLEG_BROWSER_DOWNLOAD_DIR",
            "BROWSER_DOWNLOAD_DIR",
        ),
    )
    page_load_timeout: int = Field(
        default=180,
        validation_alias=AliasChoices("TRANSLEG_PAGE_LOAD_TIMEOUT"),
    )
    default_wait_timeout: int = Field(
        default=20,
        validation_alias=AliasChoices("TRANSLEG_DEFAULT_WAIT_TIMEOUT"),
    )
    download_timeout: int = Field(
        default=90,
        validation_alias=AliasChoices("TRANSLEG_DOWNLOAD_TIMEOUT"),
    )
    log_level: str = Field(
        default="INFO",
        validation_alias=AliasChoices("TRANSLEG_LOG_LEVEL"),
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def resolved_download_dir(self) -> Path:
        return self.browser_download_dir.expanduser().resolve()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

