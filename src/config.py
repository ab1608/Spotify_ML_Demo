from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE: Path = Path(__file__).parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE)

    database_url: str
    spotify_client_id: str
    spotify_client_secret: str


settings = Settings()
