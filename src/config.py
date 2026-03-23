from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).parent.parent


class ProjectSecrets(BaseSettings):
    model_config = SettingsConfigDict(env_file=PROJECT_ROOT / ".env")

    spotify_client_id: str
    spotify_client_secret: str
    database_url: str


class ProjectIO:
    data: Path = PROJECT_ROOT / "data"
    raw_data: Path = data / "raw"
    processed_data: Path = data / "processed"
    logs: Path = PROJECT_ROOT / "logs"
    log_config: Path = logs / "stderr_log_config.json"
    log_file: Path = logs / "error.log.jsonl"


secrets = ProjectSecrets()
project_io = ProjectIO()
