from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_COLUMN_ALIASES: dict[str, str] = {
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

_SELECT_CLAUSE = ",\n    ".join(
    f'"{raw}" AS {alias}' for raw, alias in _COLUMN_ALIASES.items()
)


class SpotifyDB:
    """
    Load Spotify streaming history JSON files into DuckDB.

    Instances manage a DuckDB connection and provide convenience methods for
    loading raw streaming-history JSON files and querying the resulting
    ``streams`` table.

    Attributes:
        data_dir (Path): Directory that contains the JSON files.
        glob (str): Glob pattern used to discover JSON files in ``data_dir``.

    """

    def __init__(
        self,
        data_dir: str | Path = "data",
        glob: str = "*.json",
        db_path: str = ":memory:",
    ) -> None:
        self.data_dir = Path(data_dir)
        self.glob = glob
        self._con = duckdb.connect(db_path)

    _PROCESSED_LOG_TABLE = "ingested_files"

    def _track_processed_files(self) -> None:
        """Create a table used to track files that have been ingested."""
        self._con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._PROCESSED_LOG_TABLE} (
                filename TEXT PRIMARY KEY,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _get_new_files(self, data_files: list[Path]) -> list[Path]:
        """
        Return only files that are not present in the ingestion log.

        Args:
            data_files (list[Path]): Candidate files discovered on disk.

        Returns:
            list[Path]: Files that have not been recorded as processed.

        """
        processed = self._con.execute(
            f"SELECT filename FROM {self._PROCESSED_LOG_TABLE}"
        ).fetchall()
        processed_set = {row[0] for row in processed}
        return [f for f in data_files if f.name not in processed_set]

    def load(self) -> SpotifyDB:
        """
        Discover JSON files and load them into the ``streams`` table.

        Returns:
            SpotifyDB: The current instance for method chaining.

        Raises:
            FileNotFoundError: If no matching files are found in ``data_dir``.

        """
        self._track_processed_files()
        data_files = self.data_dir.glob(self.glob)

        if not data_files:
            msg = f"Did not find any files in {self.data_dir} matching the glob expression {self.glob}"
            raise FileNotFoundError(msg)

        files_to_process: list[Path] = self._get_new_files(data_files)
        if not files_to_process:
            row_count = self._con.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
            logger.info(
                "No new files to load. Database `streams` table currently contains %d rows.",
                row_count,
            )
            return self

        # Build a comma-separated quoted glob that DuckDB can read in one shot.
        pattern = str(self.data_dir / self.glob).replace("\\", "/")

        create_table = f"""
            CREATE OR REPLACE TABLE streams AS
            SELECT
                {_SELECT_CLAUSE}
            FROM read_json_auto(
                ?,
                maximum_object_size = 33554432,
                union_by_name       = true,
                filename            = false
            )
        """

        self._con.execute(create_table, [pattern])
        row_count = self._con.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
        for fp in files_to_process:
            self._con.execute(
                f"INSERT INTO {self._PROCESSED_LOG_TABLE} (filename) VALUES (?)",
                [fp.name],
            )
        logger.info("Loading %d rows.", row_count)
        return self

    def query(self, sql: str) -> duckdb.DuckDBPyConnection:
        """
        Execute SQL and return a DuckDB relation.

        The returned relation supports methods like ``.df()``, ``.pl()``, and
        ``.fetchall()``.

        Args:
            sql (str): SQL statement to execute. The main table is ``streams``.

        Returns:
            duckdb.DuckDBPyRelation: Query result relation.

        """
        return self._con.execute(sql)

    def schema(self) -> None:
        """Display the ``streams`` table schema."""
        self._con.execute("DESCRIBE streams").fetchone()

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._con.close()
