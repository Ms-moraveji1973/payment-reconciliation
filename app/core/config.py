from pydantic_settings import BaseSettings , SettingsConfigDict
from functools import lru_cache
from pydantic import Field
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[2]

class Settings(BaseSettings):
    api_v1_str: str = "/api/v1"
    DATABASE_URL: str = Field(alias="DATABASE_URL")

    SECRET_KEY : str = Field(alias="SECRET_ACCESS_KEY")
    REFRESH_SECRET_KEY : str = Field(alias="SECRET_REFRESH_KEY")
    ALGORITHM: str = "HS256"

    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 30

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        extra="ignore",
    )


@lru_cache
def get_settings():
    return Settings()