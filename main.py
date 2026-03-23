import atexit
import json
import logging.config

from spotify_handler import SpotifyHandler
from src.config import project_io, secrets
from src.database import SpotifyDB
from src.preprocess import create_user_features, get_audio_features

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
    db = SpotifyDB(
        data_dir=project_io.raw_data, db_path=secrets.database_url, glob="*.json"
    )

    # 3. Create user features
    df = db.query("SELECT * FROM streams").df()
    df = create_user_features(df)

    # 4. Get audio features for all unique tracks in the dataset
    sp = SpotifyHandler(
        client_id=secrets.spotify_client_id, client_secret=secrets.spotify_client_secret
    )
    df = get_audio_features(sp, df)

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
