import json
from datetime import datetime, timezone
from pathlib import Path
import logging
import requests # to query APIs

from crossref_pipeline.paths import RAW_DIR
from crossref_pipeline.utils.logging_utils import log_stage

# Logger for this module
logger = logging.getLogger(__name__)

# CrossRef API endpoint for publication metadata
CROSSREF_URL = "https://api.crossref.org/works"


def utc_now_string() -> str:
    """
    Return the current UTC time as a compact string.
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


@log_stage
def fetch_crossref_works(rows: int = 200) -> tuple[dict, str]:
    """
    Fetch recent publication records from the CrossRef API.

    Parameters:
    rows: Number of records to fetch.

    Returns:
    payload:
        Parsed JSON response as a Python dictionary.
    request_url:
        Final request URL.
    """

    # Parameters for the query
    query_parameters = {
        "sort": "published",
        "order": "desc",
        "rows": rows,
    }

    # Send GET request to CrossRef.
    # timeout avoids hanging forever if the API is slow.
    response = requests.get(
        CROSSREF_URL,
        params=query_parameters,
        timeout=30,
    )

    # Raise an error if the request failed.
    response.raise_for_status()

    # Return both the data and the final URL used for the request.
    return response.json(), response.url


@log_stage
def save_raw_json(payload: dict, output_path: Path) -> None:
    """
    Save the raw API response as a JSON file.
    """

    # Make sure the output folder exists.
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save JSON with UTF-8 encoding.
    # ensure_ascii=False keeps non-English characters readable.
    # indent=2 makes the file human-readable.
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


@log_stage
def run_raw_ingestion(rows: int = 200) -> Path:
    """
    Run the raw ingestion step.
    Saves the raw JSON response and returns the path to the saved file.
    """

    # Timestamp used in the output filename.
    timestamp = utc_now_string()

    # Fetch data from CrossRef.
    payload, request_url = fetch_crossref_works(rows=rows)

    # Build output path for the raw JSON file.
    output_path = RAW_DIR / f"crossref_raw_{timestamp}.json"

    # Save raw response.
    save_raw_json(payload, output_path)

    # Log useful run information.
    logger.info("Saved raw CrossRef response to: %s", output_path)
    logger.info("Request URL: %s", request_url)

    return output_path