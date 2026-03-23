import logging
import time

import pandas as pd
from requests import get as http_get

from src.spotify import Spotify

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

# Spotify enforces a rolling rate limit; ~180 req/min is a safe ceiling.
# At 0.35s delay that's ~171 req/min, leaving a small buffer.
_DEFAULT_REQUEST_DELAY: float = 0.35


def create_features(data: pd.DataFrame) -> pd.DataFrame:
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

    data["time_of_day"] = (
        pd.cut(
            data["hour"],
            [0, 6, 12, 18, 24],
            right=False,
            labels=["Night", "Morning", "Afternoon", "Evening"],
        ),
    )

    # User-behavior features
    data["did_skip"] = data["seconds"] < 3

    return data


def fetch_track_durations(
    spotify: Spotify,
    track_ids: list[str],
    request_delay: float = _DEFAULT_REQUEST_DELAY,
    max_retries: int = 3,
    retry_backoff: float = 2.0,
) -> dict[str, int | None]:
    """
    Fetch duration_ms for each track ID via GET /v1/tracks/{id}.

    Args:
        spotify: An authenticated Spotify instance.
        track_ids: List of Spotify track IDs to look up.
        request_delay: Seconds to wait between successful requests.
            Default is 0.35s (~171 req/min), safely under Spotify's limit.
        max_retries: How many times to retry a failed request before
            marking the track as failed (None).
        retry_backoff: Multiplier applied to the wait time on each
            consecutive retry for the same track.

    Returns:
        A dict mapping each track_id to its duration in milliseconds,
        or None if the track could not be fetched after all retries.

    """
    results: dict[str, int | None] = {}
    total = len(track_ids)

    logger.info("Fetching durations for %d tracks (one request each).", total)

    for idx, track_id in enumerate(track_ids, start=1):
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        delay = request_delay
        duration_ms: int | None = None

        for attempt in range(1, max_retries + 1):
            try:
                headers = spotify.get_auth_header()
                response = http_get(url, headers=headers, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    duration_ms = data.get("duration_ms")
                    break

                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", delay))
                    logger.warning(
                        "[%d/%d] %s — rate limited. Waiting %ds (attempt %d/%d).",
                        idx,
                        total,
                        track_id,
                        retry_after,
                        attempt,
                        max_retries,
                    )
                    time.sleep(retry_after)
                    delay *= retry_backoff

                elif response.status_code == 401:
                    logger.info(
                        "[%d/%d] %s — token expired, refreshing.", idx, total, track_id
                    )
                    spotify.token = spotify.get_token()
                    # Don't sleep or increment backoff; just retry immediately.

                else:
                    logger.warning(
                        "[%d/%d] %s — HTTP %d on attempt %d/%d.",
                        idx,
                        total,
                        track_id,
                        response.status_code,
                        attempt,
                        max_retries,
                    )
                    time.sleep(delay)
                    delay *= retry_backoff

            except Exception as exc:
                logger.exception(
                    "[%d/%d] %s — exception on attempt %d/%d: %s",
                    idx,
                    total,
                    track_id,
                    attempt,
                    max_retries,
                    exc,
                )
                time.sleep(delay)
                delay *= retry_backoff

        if duration_ms is None:
            logger.error(
                "[%d/%d] %s — giving up after %d retries.",
                idx,
                total,
                track_id,
                max_retries,
            )

        results[track_id] = duration_ms
        time.sleep(request_delay)

    failed = [tid for tid, dur in results.items() if dur is None]
    if failed:
        logger.warning("%d track(s) could not be fetched: %s", len(failed), failed)
    else:
        logger.info("All %d tracks fetched successfully.", total)

    return results


# --- Example usage ---
if __name__ == "__main__":
    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"

    # Replace with your actual list of track IDs
    track_ids: list[str] = [
        "11dFghVXANMlKmJXsNCbNl",
        "2TpxZ7JUBn3uw46aR7qd6V",
        # ... rest of your IDs
    ]

    sp = Spotify(CLIENT_ID, CLIENT_SECRET)
    durations = fetch_track_durations(sp, track_ids)

    for track_id, duration_ms in durations.items():
        if duration_ms is not None:
            minutes, seconds = divmod(duration_ms // 1000, 60)
            logger.info(f"{track_id}: {minutes}:{seconds:02d}")
        else:
            logger.warning(f"{track_id}: FAILED")
