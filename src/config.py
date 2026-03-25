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


SPOTIFY_EXPORT_COLUMN_ALIASES: dict[str, str] = {
    "ts": "timestamp",
    "platform": "platform",
    "ms_played": "ms_played",
    "conn_country": "country",
    "ip_addr": "ip_address",
    "master_metadata_track_name": "track_name",
    "master_metadata_album_artist_name": "artist_name",
    "master_metadata_album_album_name": "album_name",
    "spotify_track_uri": "track_uri",
    "episode_name": "episode_name",
    "episode_show_name": "episode_show_name",
    "spotify_episode_uri": "episode_uri",
    "audiobook_title": "audiobook_title",
    "audiobook_uri": "audiobook_uri",
    "audiobook_chapter_uri": "audiobook_chapter_uri",
    "audiobook_chapter_title": "audiobook_chapter_title",
    "reason_start": "reason_start",
    "reason_end": "reason_end",
    "shuffle": "shuffle",
    "skipped": "skipped",
    "offline": "offline",
    "offline_timestamp": "offline_timestamp",
    "incognito_mode": "incognito_mode",
}


secrets = ProjectSecrets()
project_io = ProjectIO()
