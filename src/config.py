from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=PROJECT_ROOT / ".env")

    database_url: str
    spotify_client_id: str
    spotify_client_secret: str
    log_config: str
    log_file: str


settings = Settings()
