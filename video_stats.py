import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
FOR_HANDLE = "naveenautomationlabs"

CHANNELS_LIST_URL = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={FOR_HANDLE}&key={API_KEY}"


def get_playlist_id():
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


if __name__ == "__main__":
    playlist_id = get_playlist_id()
    print(f"Playlist ID: {playlist_id}")
