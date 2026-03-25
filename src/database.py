from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import duckdb

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from pathlib import Path

    from pandas import DataFrame


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
        db_path: str = ":memory:",
    ) -> None:

        self._con = duckdb.connect(db_path)

    def _check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table already exists in the database.

        Args:
            table_name (str): Table to check for existence.

        Returns:
            bool: True if the table exists, False otherwise.

        """
        existing_tables = self._con.execute("SHOW TABLES").fetchall()
        existing_table_names = {table[0] for table in existing_tables}
        return table_name in existing_table_names

    def _import_file(self, file_path: Path, table_name: str, glob: str | None = None) -> None:
        """
        Import a JSON, CSV or Parquet file or directory of files to the `table_name`.

        Args:
            file_path (Path): The path to a single file or a directory containing files to import.
            If a directory is provided, the `glob` parameter will be used to match files within that directory.
            table_name (str): The name of the table to create or replace with the imported data.
            glob(str | None): The glob pattern to match files (e.g., "*.json").
            If multiple files match the glob, they will be imported together.

        Raises:
            ValueError: If the file extension is not supported.

        """
        if glob:
            if file_path.is_dir():
                file_path = file_path / glob
            else:
                msg = f"Provided file_path {file_path} is not a directory, but a glob pattern {glob} was provided. Please provide a directory path when using glob patterns."
                raise ValueError(msg)

        file_extension = file_path.suffix.lower()

        if file_extension == ".json":
            read_command = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT
                   *
                FROM read_json_auto(
                    ?,
                    maximum_object_size = 33554432,
                    union_by_name       = true,
                    filename            = false
                )
            """
        elif file_extension == ".csv":
            read_command = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT
                   *
                FROM read_csv(?, union_by_name = true)
            """
        elif file_extension == ".parquet":
            read_command = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT
                   *
                FROM read_parquet(?)
            """
        else:
            msg = f"Unsupported file extension: {file_extension}. Supported extensions are .json, .csv, .parquet"
            raise ValueError(msg)

        self._con.execute(read_command, [str(file_path)])

        # Check how many
        # rows were imported and log a warning if the table is empty
        row_count = self._con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        if row_count == 0:
            logger.warning("Table %s is empty after import.", table_name)
        else:
            logger.info("Imported %d rows into table %s.", row_count, table_name)

    def import_data(
        self,
        file_path: Path,
        target_table: str,
        glob: str | None = None,
        *,
        force: bool = False,
    ) -> None:
        """
        Import data from a file or directory into a DuckDB table.

        Args:
            file_path (Path): The path to the file or directory to import.
            target_table (str): The name of the target table in DuckDB.
            glob (str | None): Optional glob pattern to match files if `file_path` is a directory.
            force (bool): If True, overwrite the target table if it already exists. Defaults to False.

        """
        if not force and self._check_table_exists(target_table):
            msg = f"Table '{target_table}' already exists. Use force=True to overwrite it."
            # Don't raise an exception, just let the user know and skip the import
            logger.warning(msg)
            return

        self._import_file(file_path, target_table, glob)

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

    def insert_table(self, df: DataFrame, table_name: str, *, force: bool = False) -> None:
        """
        Insert a DataFrame into the DuckDB instance as a new table.

        Args:
            df (pd.DataFrame): The DataFrame to insert.
            table_name (str): The name of the new table to create in DuckDB.
            force (bool, optional): If True, overwrite the table if it exists. Defaults to False.

        """
        if not force and self._check_table_exists(table_name):
            msg = f"Table '{table_name}' already exists. Use force=True to overwrite it."
            raise ValueError(msg)

        self._con.register("temp_df", df)
        self._con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
        self._con.unregister("temp_df")

    def schema(self) -> None:
        """Display the ``streams`` table schema."""
        self._con.execute("DESCRIBE streams").fetchone()

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._con.close()
