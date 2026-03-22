from pathlib import Path

from src.config import settings
from src.database import SpotifyDB


def run_pipeline() -> None:

    # 1. Extract raw data from source.
    # In this case, data was provided by Spotify in a set of JSON files
    # so we do not have to do our own extraction from any source system
    data_dir: Path = Path(__file__).parent / "data"
    raw_data: Path = data_dir / "raw"

    # 2. Load (move) raw data to DuckDB instance
    db = SpotifyDB(data_dir=raw_data, db_path=settings.database_url, glob="*.json")
    db.load()

    # 3. Run initial transformations

    # Close database
    db.close()


def main() -> None:
    """Main entry point."""
    try:
        run_pipeline()
    except Exception as e:
        print(f"Encountered an error {e}")


if __name__ == "__main__":
    main()
