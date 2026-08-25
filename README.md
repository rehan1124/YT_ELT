# YT_ELT

Small Python script that retrieves the uploads playlist ID for a YouTube channel
using the YouTube Data API v3.

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

Edit `.env` so it contains:

```dotenv
API_KEY=your-youtube-data-api-key
```

Create the key in Google Cloud Console, enable **YouTube Data API v3** for the
project, and keep the key private. The `.env` file is intended for local use and
must not be committed.

## Run

With the virtual environment activated and `.env` configured:

```cmd
python video_stats.py
```

The script prints the channel's uploads playlist ID. The channel handle is
currently configured in `video_stats.py` as `naveenautomationlabs`.
