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

        json_response = response.json()

        channel_items = json_response.get("items", [])
        channel_playlist_id = channel_items[0]["contentDetails"]["relatedPlaylists"][
            "uploads"
        ]

        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        # https://requests.readthedocs.io/en/latest/user/quickstart/#errors-and-exceptions
        raise e


def get_playlist_items(playlist_id, page_token=None):
    """
    Get the items in a playlist using the YouTube Data API.
    Args:
        playlist_id (str): The ID of the playlist.
        page_token (str, optional): The token for the next page of results.
    Returns:
        list: A list of playlist items with additional information.
    """
    try:
        response = requests.get(
            f"{YT_URL}/playlistItems?part={PART}&playlistId={playlist_id}&maxResults={MAX_RESULTS}&key={API_KEY}&pageToken={page_token if page_token else ''}"
        )
        response.raise_for_status()

        json_response = response.json()

        playlist_items = json_response
        return playlist_items
    except requests.exceptions.RequestException as e:
        raise e


if __name__ == "__main__":

    playlist_id = get_playlist_id()

    playlist_items = get_playlist_items(playlist_id)

    next_page_token = playlist_items.get("nextPageToken")

    video_items = playlist_items.get("items", [])
    video_ids = [item["contentDetails"]["videoId"] for item in video_items]

    while next_page_token:
        playlist_items = get_playlist_items(playlist_id, next_page_token)

        next_page_token = playlist_items.get("nextPageToken")

        video_items.extend(playlist_items.get("items", []))
        video_ids.extend(
            [
                item["contentDetails"]["videoId"]
                for item in playlist_items.get("items", [])
            ]
        )

    print(f"Total Video IDs: {len(video_ids)}")
    print(f"Video IDs: {video_ids}")
