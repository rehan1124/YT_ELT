# YT_ELT

Small Python script that collects the video IDs for a YouTube channel using the
YouTube Data API v3. It looks up the channel's uploads playlist, then pages
through that playlist to gather every video ID.

## Requirements

- Python 3.10 or newer
- A Google API key with the YouTube Data API v3 enabled

This project was created using Python 3.14.6.

## Setup

From the repository directory, create and activate a virtual environment:

```cmd
python -m venv venv
venv\Scripts\activate.bat
```

Install all project dependencies from the requirements file:

```cmd
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

After activating the virtual environment, the shorter `pip` form can also be
used:

```cmd
pip install --upgrade pip
pip install -r requirements.txt
```

Both forms install the same packages. `pip install` is shorter, while
`python -m pip` explicitly runs pip through the selected Python interpreter and
helps avoid using a different Python installation's pip. The `python -m pip`
form is recommended when troubleshooting environment or interpreter issues.

The same dependency installation can be run again safely after pulling changes.

## Environment configuration

Use `.env.example` as the reference for the required local configuration. Copy
it to `.env` if `.env` does not already exist, then set the API key value:

```cmd
copy .env.example .env
```

Edit `.env` so it contains both required values:

```dotenv
API_KEY=your-youtube-data-api-key
YT_URL=https://youtube.googleapis.com/youtube/v3
```

`API_KEY` is your Google API key and `YT_URL` is the base URL of the YouTube
Data API v3. Both are read at startup, and requests fail if either is missing.

Create the key in Google Cloud Console, enable **YouTube Data API v3** for the
project, and keep the key private. The `.env` file is intended for local use and
must not be committed.

## Run

With the virtual environment activated and `.env` configured:

```cmd
python video_stats.py
```

The script resolves the channel's uploads playlist, then requests the playlist
items 50 at a time until every page has been read. Progress is written to the
log as each page arrives, for example:

```text
2026-09-03 10:15:04,131 - Fetched page 1: 50 videos of 412
2026-09-03 10:15:04,742 - Fetched page 2: 100 videos of 412 (24%)
```

When paging finishes it logs the total count and the full list of video IDs.

The channel handle is currently configured in `video_stats.py` as
`naveenautomationlabs`.
