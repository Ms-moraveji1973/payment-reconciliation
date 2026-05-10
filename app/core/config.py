from pydantic_settings import BaseSettings , SettingsConfigDict
from functools import lru_cache
from pydantic import Field
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[2]

class Settings(BaseSettings):
    api_v1_str: str = "/api/v1"
    DATABASE_URL: str = Field(alias="DATABASE_URL")

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        extra="ignore",
    )


@lru_cache
def get_settings():
    return Settings()