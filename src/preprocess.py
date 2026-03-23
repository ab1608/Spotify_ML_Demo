import json
import logging
import time

import pandas as pd

from spotify_handler import SpotifyHandler
from src.config import project_io

logger = logging.getLogger(__name__)

month_to_season = {
    1: "Winter",
    2: "Winter",
    3: "Spring",
    4: "Spring",
    5: "Spring",
    6: "Summer",
    7: "Summer",
    8: "Summer",
    9: "Autumn",
    10: "Autumn",
    11: "Autumn",
    12: "Winter",
}


def create_user_features(data: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" not in data:
        msg = f"Expected 'timestamp' column for feature extraction, but not found. Columns: {data.columns.tolist()}"
        logger.error(msg)
        raise pd.errors.DataError(msg)

    # Time features
    data = data.assign(
        year=data["timestamp"].dt.year,
        month=data["timestamp"].dt.month,
        month_name=data["timestamp"].dt.month_name(),
        hour=data["timestamp"].dt.hour,
        minute=data["timestamp"].dt.minute,
        seconds=data["timestamp"].dt.second,
        quarter="Q" + data["timestamp"].dt.quarter.astype("string"),
        season=data["timestamp"].dt.month.map(month_to_season),
        is_weekend=data["timestamp"].dt.day_of_week > 4,  # Monday=0, Sunday=6
    )
    # Additional features
    data = data.assign(
        tie_of_day=pd.cut(
            data["hour"],
            [0, 6, 12, 18, 24],
            right=False,
            labels=["Night", "Morning", "Afternoon", "Evening"],
        ),
        did_skip=data["seconds"] < 3,
    )

    return data


def get_audio_features(sp: SpotifyHandler, data: pd.DataFrame) -> pd.DataFrame:
    if "track_uri" not in data:
        msg = f"Expected 'track_uri' column for feature extraction, but not found. Columns: {data.columns.tolist()}"
        logger.error(msg)
        raise pd.errors.DataError(msg)

    # Extract unique track URIs to minimize redundant API calls
    data["track_uri"] = data["track_uri"].astype(str)  # Ensure URIs are strings
    # Remove "spotify:track:" prefix if present to get just the ID for API calls
    data["track_uri"] = data["track_uri"].str.replace("spotify:track:", "", regex=False)
    unique_tracks = data["track_uri"].unique().tolist()
    logger.info(
        "Extracted %d unique track URIs for feature fetching.", len(unique_tracks)
    )
    track_durations = fetch_track_durations(sp, unique_tracks)

    # Map durations back to the original DataFrame
    data["duration_ms"] = data["track_uri"].map(track_durations)

    return data


def fetch_track_durations(
    spotify: SpotifyHandler,
    track_uris: list[str],
    request_delay: float = 0.35,
    max_retries: int = 3,
    retry_backoff: float = 2.0,
) -> dict[str, int | None]:
    """
    Fetch duration_ms for each track URI via GET /v1/tracks/{id}.

    Args:
        spotify: An authenticated Spotify instance.
        track_uris: List of Spotify track URIs to look up.
        request_delay: Seconds to wait between successful requests.
            Default is 0.35s (~171 req/min), safely under Spotify's limit.
        max_retries: How many times to retry a failed request before
            marking the track as failed (None).
        retry_backoff: Multiplier applied to the wait time on each
            consecutive retry for the same track.

    Returns:
        A dict mapping each track_uri to its duration in milliseconds,
        or None if the track could not be fetched after all retries.

    """
    # Check if we have a cached result from a previous run to avoid unnecessary API calls
    track_duration_cache = project_io.processed_data / "track_durations_cache.json"
    if track_duration_cache.exists():
        with track_duration_cache.open() as f_in:
            cached_durations = json.load(f_in)
        # Remove already cached tracks from the list to fetch
        track_uris = [uri for uri in track_uris if uri not in cached_durations]
        logger.info(
            "Loaded durations for %d tracks from cache. %d tracks remain to fetch.",
            len(cached_durations),
            len(track_uris),
        )
    results: dict[str, int | None] = {}
    total = len(track_uris)

    logger.info("Fetching durations for %d tracks (one request each).", total)

    for idx, track_uri in enumerate(track_uris, start=1):
        for attempt in range(1, max_retries + 1):
            track_data = spotify.get_track(track_uri)
            if track_data and "duration_ms" in track_data:
                duration_ms = track_data["duration_ms"]
                logger.info(
                    "[%d/%d] %s — fetched duration %d ms on attempt %d.",
                    idx,
                    total,
                    track_uri,
                    duration_ms,
                    attempt,
                )
                results[track_uri] = duration_ms
                time.sleep(request_delay)  # Wait before next request
                break  # Exit retry loop on success

            if track_data is None:
                logger.warning(
                    "[%d/%d] %s — rate limited or other error on attempt %d/%d.",
                    idx,
                    total,
                    track_uri,
                    attempt,
                    max_retries,
                )
                results[track_uri] = None
                time.sleep(
                    request_delay * retry_backoff ** (attempt - 1)
                )  # Wait before retrying

    failed = [tid for tid, dur in results.items() if dur is None]
    if failed:
        logger.warning("%d track(s) could not be fetched: %s", len(failed), failed)
    else:
        logger.info("All %d tracks fetched successfully.", total)

    # Append new results to cache file for future runs
    with track_duration_cache.open("a") as f_out:
        json.dump(results, f_out, indent=2)

    return results
