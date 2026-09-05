import requests
import logging
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
YT_URL = os.getenv("YT_URL")
FOR_HANDLE = "naveenautomationlabs"
MAX_RESULTS = 50
PART = "contentDetails"
TIMEOUT = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

if not API_KEY or not YT_URL:
    raise SystemExit("API_KEY and YT_URL must be set in the environment or .env file")


def get_playlist_id():
    """
    Get the playlist ID of the channel's uploads playlist using the YouTube Data API.
    Returns:
        str: The playlist ID of the channel's uploads playlist.
    """
    try:
        response = requests.get(
            f"{YT_URL}/channels",
            params={"part": PART, "forHandle": FOR_HANDLE, "key": API_KEY},
            timeout=TIMEOUT,
        )
        response.raise_for_status()

        channel_items = response.json().get("items", [])
        if not channel_items:
            raise ValueError(f"No channel found for handle '{FOR_HANDLE}'")

        return channel_items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
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
        dict: The raw playlist-items API response.
    """
    params = {
        "part": PART,
        "playlistId": playlist_id,
        "maxResults": MAX_RESULTS,
        "key": API_KEY,
    }
    if page_token:
        params["pageToken"] = page_token

    try:
        response = requests.get(
            f"{YT_URL}/playlistItems", params=params, timeout=TIMEOUT
        )
        response.raise_for_status()

        return response.json()
    except requests.exceptions.RequestException as e:
        raise e


def _get_video_ids(playlist_items):
    """Return video IDs from a YouTube playlist-items response."""
    return [
        item["contentDetails"]["videoId"] for item in playlist_items.get("items", [])
    ]


def get_all_video_ids(playlist_id):
    """Return every video ID in the playlist, following pagination."""
    video_ids = []
    page_token = None
    page_number = 0
    total_results = None

    while True:
        playlist_items = get_playlist_items(playlist_id, page_token)
        if total_results is None:
            total_results = playlist_items.get("pageInfo", {}).get("totalResults")

        video_ids.extend(_get_video_ids(playlist_items))
        page_number += 1
        progress = (
            f" of {total_results} ({len(video_ids) / total_results:.0%})"
            if total_results
            else ""
        )
        logging.info(
            "Fetched page %s: %s videos%s", page_number, len(video_ids), progress
        )

        page_token = playlist_items.get("nextPageToken")
        if not page_token:
            return video_ids


def batch_list(video_ids, batch_size=50):
    """Yield successive batches of video IDs."""
    for i in range(0, len(video_ids), batch_size):
        yield video_ids[i : i + batch_size]


def extract_video_stats(video_ids):
    """Extract video statistics for a list of video IDs."""
    stats = []

    try:
        for batch_number, batch in enumerate(batch_list(video_ids), start=1):
            video_ids_str = ",".join(batch)
            response = requests.get(
                f"{YT_URL}/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}",
                timeout=TIMEOUT,
            )
            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                stats.append(
                    {
                        "video_id": item["id"],
                        "title": item["snippet"]["title"],
                        "published_at": item["snippet"]["publishedAt"],
                        "duration": item["contentDetails"]["duration"],
                        "view_count": item["statistics"].get("viewCount", 0),
                        "like_count": item["statistics"].get("likeCount", 0),
                        "comment_count": item["statistics"].get("commentCount", 0),
                    }
                )

            logging.info(
                "Fetched batch %s: %s of %s stats (%.0f%%)",
                batch_number,
                len(stats),
                len(video_ids),
                100 * len(stats) / len(video_ids),
            )

        return stats
    except requests.exceptions.RequestException as e:
        raise e


def _save_stats_to_file(stats):
    """Save the extracted video statistics to a JSON file."""
    import json
    import datetime

    filename = f"./data/logs/yt_stats_{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json"

    with open(filename, "w") as f:
        json.dump(stats, f, indent=4)
    logging.info("Saved video stats to %s", filename)


if __name__ == "__main__":

    """
    Main execution block to fetch video statistics from a YouTube channel's uploads playlist.
    """

    playlist_id = get_playlist_id()
    video_ids = get_all_video_ids(playlist_id)

    logging.info("Total Video IDs: %s", len(video_ids))
    # logging.info("Video IDs: %s", video_ids)

    extracted_stats = extract_video_stats(video_ids)
    logging.info("Extracted Video Stats: %s", len(extracted_stats))

    _save_stats_to_file(extracted_stats)
