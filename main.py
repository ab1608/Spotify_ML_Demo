import atexit
import json
import logging.config
from pathlib import Path

from src.config import settings
from src.database import SpotifyDB

logger = logging.getLogger(__name__)  # __name__ is a common choice


def setup_logging():
    config_file = Path(settings.log_config)
    with config_file.open() as f_in:
        config = json.load(f_in)

    logging.config.dictConfig(config)
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)


def run_pipeline() -> None:

    # 1. Extract raw data from source.
    # In this case, data was provided by Spotify in a set of JSON files
    # so we do not have to do our own extraction from any source system
    data_dir: Path = Path(__file__).parent / "data"
    raw_data: Path = data_dir / "raw"

    # 2. Load (move) raw data to DuckDB instance
    db = SpotifyDB(data_dir=raw_data, db_path=settings.database_url, glob="*.json")

    # 3. Run initial transformations

    # Close database
    db.close()


def main() -> None:
    """Main entry point."""
    setup_logging()
    try:
        run_pipeline()
    except Exception:
        logger.exception("Encountered an error during pipeline execution")


if __name__ == "__main__":
    main()
