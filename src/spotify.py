import json
from typing import Any

import requests as r
from requests import get, post


class Spotify:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
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
        result = post(url, headers=headers, data=data)
        json_results = json.loads(result.content)
        token = json_results["access_token"]
        return token

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def check_response(self, res: r.Response) -> Any | None:
        """Check the response and return the decoded JSON, if available.

        Args:
            res (r.Response): A Response object.

        Returns:
            Any | None:
        """
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 401:
            print("[%d/%d] %s — token expired.")
        else:
            print(f"[%d/%d] %s — Spotify responded with {res.status_code}")

    # Get Spotify catalog information for a single artist identified
    # by their unique Spotify ID.
    def get_artist(self, artist_id: str):
        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        header = self.get_auth_header()

        result = get(url, headers=header)
        return json.loads(result.content)

    # Get Spotify catalog information about albums, artists, playlists,
    # tracks, shows, episodes or audiobooks that match a keyword string.
    # Audiobooks are only available within the US, UK, Canada, Ireland,
    # New Zealand and Australia markets
    def search_item(self, item: str, type: str, limit: int):
        url: str = "https://api.spotify.com/v1/search"
        query: str = f"?q={item}&type={type}&limit={limit}"
        query_url: str = f"{url}{query}"
        headers = self.get_auth_header()
        result = get(query_url, headers=headers)
        return json.loads(result.content)

    # Source: https://developer.spotify.com/documentation/web-api/reference/get-track
    def get_track(self, track_id: str):
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        header = self.get_auth_header()
        result = get(url, headers=header)
        return self.check_response(result)
