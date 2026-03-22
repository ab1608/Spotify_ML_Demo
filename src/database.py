from __future__ import annotations

from pathlib import Path

import duckdb

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
    Reads Spotify streaming-history JSON files from *data_dir*
    into an in-memory (or file-based) DuckDB database.

    Parameters
    ----------
    data_dir : str | Path
        Directory that contains the JSON files.  Defaults to ``data/``.
    glob : str
        Glob pattern used to discover JSON files inside *data_dir*.
        Defaults to ``"*.json"``.
    db_path : str
        DuckDB database path.  Use ``":memory:"`` (default) for an
        in-memory database, or supply a file path to persist it.

    Examples
    --------
    >>> db = SpotifyDB()
    >>> db.load()
    Loaded 42 317 rows from 15 file(s).
    >>> top = db.query("SELECT artist_name, COUNT(*) AS plays FROM streams GROUP BY 1 ORDER BY 2 DESC LIMIT 5")
    >>> print(top)
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
        """Creates a table to track files we have already processed."""
        self._con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._PROCESSED_LOG_TABLE} (
                filename TEXT PRIMARY KEY,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _get_new_files(self, data_files: list[Path]) -> list[Path]:
        """Filters out files that are already in the database log."""
        processed = self._con.execute(
            f"SELECT filename FROM {self._PROCESSED_LOG_TABLE}"
        ).fetchall()
        processed_set = {row[0] for row in processed}
        return [f for f in data_files if f.name not in processed_set]

    def load(self) -> SpotifyDB:
        """
        Discover JSON files and load them to the ``streams`` table.

        Parameters
        ----------
        force : bool
            Re-create the table even if it already exists.
        """
        self._track_processed_files()
        data_files = self.data_dir.glob(self.glob)

        if not data_files:
            raise FileNotFoundError(
                f"Did not find any files in {self.data_dir} matching the glob expression {self.glob}"
            )

        files_to_process: list[Path] = self._get_new_files(data_files)
        if not files_to_process:
            row_count = self._con.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
            print(
                f"No new files to load. Database `streams` table currently contains {row_count:,} rows."
            )
            return self

        # Build a comma-separated quoted glob that DuckDB can read in one shot.
        pattern = str(self.data_dir / self.glob).replace("\\", "/")

        create_table = f"""
            CREATE OR REPLACE TABLE streams AS
            SELECT
                {_SELECT_CLAUSE}
            FROM read_json_auto(
                '{pattern}',
                maximum_object_size = 33554432,
                union_by_name       = true,
                filename            = false
            )
        """

        self._con.execute(create_table)
        row_count = self._con.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
        for fp in files_to_process:
            self._con.execute(
                f"INSERT INTO {self._PROCESSED_LOG_TABLE} (filename) VALUES (?)",
                [fp.name],
            )
        print(f"Loading {row_count:,} rows.")
        return self

    def query(self, sql: str) -> duckdb.DuckDBPyRelation:
        """
        Execute arbitrary SQL and return a DuckDB relation.

        The result supports ``.df()`` (pandas DataFrame), ``.pl()``
        (polars DataFrame), ``.fetchall()``, etc.

        Parameters
        ----------
        sql : str
            SQL statement.  The main table is named ``streams``.
        """
        return self._con.execute(sql)

    # helpers

    def top_artists(self, n: int = 10) -> duckdb.DuckDBPyRelation:
        """Return the *n* most-played artists by total play-time."""
        return self.query(f"""
            SELECT
                artist_name,
                COUNT(*) AS plays,
                ROUND(SUM(ms_played) / 3_600_000.0, 2) AS hours_played
            FROM streams
            WHERE artist_name IS NOT NULL
            GROUP BY artist_name
            ORDER BY hours_played DESC
            LIMIT {n}
        """)

    def top_tracks(self, n: int = 10) -> duckdb.DuckDBPyRelation:
        """Return the *n* most-played tracks by play count."""
        return self.query(f"""
            SELECT
                track_name,
                artist_name,
                COUNT(*) AS plays,
                ROUND(SUM(ms_played) / 60_000.0, 1) AS total_minutes
            FROM streams
            WHERE track_name IS NOT NULL
            GROUP BY track_name, artist_name
            ORDER BY plays DESC
            LIMIT {n}
        """)

    def listening_by_year(self) -> duckdb.DuckDBPyRelation:
        """Return total listening hours grouped by calendar year."""
        return self.query("""
            SELECT
                YEAR(TIMESTAMPTZ timestamp) AS year,
                ROUND(SUM(ms_played) / 3_600_000.0, 2) AS hours_played
            FROM streams
            GROUP BY year
            ORDER BY year
        """)

    def schema(self) -> None:
        """Print the ``streams`` table schema."""
        self._con.execute("DESCRIBE streams").fetchone()

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._con.close()

    # context-manager support
    def __enter__(self) -> SpotifyDB:
        return self

    def __exit__(self, *_) -> None:
        self.close()
