import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
YT_URL = os.getenv("YT_URL")
FOR_HANDLE = "naveenautomationlabs"
MAX_RESULTS = 50
PART = "contentDetails"

CHANNELS_LIST_URL = (
    f"{YT_URL}/channels?part={PART}&forHandle={FOR_HANDLE}&key={API_KEY}"
)


def get_playlist_id():
    """
    Get the playlist ID of the channel's uploads playlist using the YouTube Data API.
    Returns:
        str: The playlist ID of the channel's uploads playlist.
    """
    try:
        response = requests.get(CHANNELS_LIST_URL)
        response.raise_for_status()

        http_code = response.status_code
        json_response = response.json()

        # print(f"HTTP Code: {http_code}")
        # print(f"JSON Response: {json.dumps(json_response, indent=4)}")

        channel_items = json_response.get("items", [])
        channel_playlist_id = channel_items[0]["contentDetails"]["relatedPlaylists"][
            "uploads"
        ]

        # print(f"Channel items: {channel_items}")
        # print(f"Channel Playlist ID: {channel_playlist_id}")

        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        # https://requests.readthedocs.io/en/latest/user/quickstart/#errors-and-exceptions
        raise e


def get_playlist_items(playlist_id):
    """
    Get the items in a playlist using the YouTube Data API.
    Args:
        playlist_id (str): The ID of the playlist.
    Returns:
        list: A list of playlist items with additional information.
    """
    try:
        response = requests.get(
            f"{YT_URL}/playlistItems?part={PART}&playlistId={playlist_id}&maxResults={MAX_RESULTS}&key={API_KEY}"
        )
        response.raise_for_status()

        http_code = response.status_code
        json_response = response.json()

        # print(f"HTTP Code: {http_code}")
        # print(f"JSON Response: {json.dumps(json_response, indent=4)}")

        # playlist_items = json_response.get("items", [])
        playlist_items = json_response
        return playlist_items
    except requests.exceptions.RequestException as e:
        raise e


if __name__ == "__main__":

    playlist_id = get_playlist_id()
    print(f"Playlist ID: {playlist_id}")

    playlist_items = get_playlist_items(playlist_id)
    print(f"Playlist Items: {json.dumps(playlist_items, indent=4)}")
