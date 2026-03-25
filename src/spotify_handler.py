import json
import logging
from typing import Any

from requests import Response, get, post

logger = logging.getLogger(__name__)


class SpotifyHandler:
    def __init__(self, client_id: str, client_secret: str, timeout: int = 5):
        """
        Initialize the Spotify client.

        Args:
            client_id (str): The client ID for the Spotify API.
            client_secret (str): The client secret for the Spotify API.
            timeout (int, optional): The timeout for API requests in seconds. Defaults to 5.

        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.token = self.get_token()

    def get_token(self) -> str:
        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        result = post(url, headers=headers, data=data, timeout=self.timeout)
        json_results = json.loads(result.content)
        return json_results["access_token"]

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def check_response(self, res: Response) -> Any | None:
        """
        Check the response and return the decoded JSON, if available.

        Args:
            res (Response): A Response object.

        Returns:
            json_data (Any | None): The decoded JSON if the response was successful, None otherwise.

        """
        if res.status_code == 200:
            return res.json()

        if res.status_code == 401:
            logger.warning("[%d/%d] %s — token expired.", res.status_code, res.reason, res.text)
        else:
            logger.warning(
                "[%d/%d] %s — Spotify responded with %d",
                res.status_code,
                res.reason,
                res.text,
                res.status_code,
            )

        return None

    def get_artist(self, artist_id: str):
        """
        Get Spotify catalog information for a single artist identified
        by their unique Spotify ID.

        Args:
            artist_id (str): The Spotify ID of the artist.

        Returns:
            artist_info (dict | None): The artist information if available, None otherwise.

        """
        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        header = self.get_auth_header()

        result = get(url, headers=header, timeout=self.timeout)
        return self.check_response(result)

    def search_item(self, item: str, item_type: str, limit: int):
        """
        Get Spotify catalog information about albums, artists, playlists,
        tracks, shows, episodes or audiobooks that match a keyword string.
        Audiobooks are only available within the US, UK, Canada, Ireland,
        New Zealand and Australia markets
        Args:
            item (str): The search query's keywords. For example: "Muse" or "Muse Starlight".
            item_type (str): The type of item to search for. One of: "album", "artist", "playlist", "track", "show", "episode", or "audiobook".
            limit (int):  The maximum number of results to return. Must be a positive integer between 1 and 50.

        Returns:
            search_results (dict | None): The search results if the request was successful, None otherwise.

        """
        url: str = "https://api.spotify.com/v1/search"
        query: str = f"?q={item}&type={item_type}&limit={limit}"
        query_url: str = f"{url}{query}"
        headers = self.get_auth_header()
        result = get(query_url, headers=headers, timeout=self.timeout)
        return self.check_response(result)

    def get_track(self, track_id: str) -> dict | None:
        """
        Get Spotify track info from `get-track` endpoint.
        Source: https://developer.spotify.com/documentation/web-api/reference/get-track

        Args:
            track_id (str): The Spotify ID of the track.

        Returns:
            track_info (dict | None): The track information if available, None otherwise.

        """
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        header = self.get_auth_header()
        result = get(url, headers=header, timeout=self.timeout)
        return self.check_response(result)
