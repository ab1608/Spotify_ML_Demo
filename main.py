import atexit
import json
import logging.config

from src.config import project_io, secrets
from src.database import SpotifyDB
from src.preprocess import create_user_features

logger = logging.getLogger(__name__)  # __name__ is a common choice


def setup_logging():
    config_file = project_io.log_config
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

    # 2. Load (move) raw data to DuckDB instance
    db = SpotifyDB(db_path=secrets.database_url)

    # Spotify exported data
    db.import_data(
        file_path=project_io.raw_data,
        glob="*.json",
        target_table="streams",
        force=False,
    )

    # Audio features downloaded from Kaggle
    db.import_data(
        file_path=project_io.raw_data / "audio_features.csv",
        target_table="audio_features",
        force=True,
    )

    # Note: In a real-world scenario, we would have extracted the audio features
    # from an API but for the sake of this simple project, we downloaded it from Kaggle.
    # See the src/spotify.py file for how this may have been accomplished otherwise.

    # 3. Feature engineering

    # User features
    df = db.query("SELECT * FROM streams").df()
    df = create_user_features(df)

    # 5. Store the enriched dataset back to DuckDB for future use
    db.insert_table(df, "enriched_streams")

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
