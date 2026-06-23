import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from crossref_pipeline.paths import STAGING_DIR
from crossref_pipeline.utils.logging_utils import log_stage


logger = logging.getLogger(__name__)


# Fields that we explicitly extract into the staging record.
# Rare fields will go to extra_metadata.
KNOWN_FIELDS = {
    "indexed",
    "reference-count",
    "references-count",
    "publisher",
    "content-domain",
    "DOI",
    "type",
    "created",
    "source",
    "is-referenced-by-count",
    "prefix",
    "member",
    "deposited",
    "score",
    "resource",
    "issued",
    "URL",
    "published",
    "published-print",
    "published-online",
    "container-title",
    "short-container-title",
    "title",
    "ISSN",
    "issn-type",
    "author",
    "volume",
    "issue",
    "page",
    "journal-issue",
    "link",
    "license",
    "abstract",
    "alternative-id",
    "ISBN",
    "isbn-type",
    "language",
    "article-number"
}


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def first_from_list(value: Any) -> str | None:
    """Return first item from a CrossRef list field."""
    if isinstance(value, list) and value:
        return str(value[0])
    return None


def normalize_doi(value: Any) -> str | None:
    """Normalize DOI."""
    if not value:
        return None
    return str(value).strip().lower()


def parse_date_parts(date_obj: Any) -> tuple[str | None, str | None]:
    """
    Convert CrossRef date-parts into YYYY-MM-DD and store precision (because sometimes only a year is provided).

    Important: this does not validate if the year is realistic.
    Data-quality checks like year > current year happens later in silver.
    """
    if not isinstance(date_obj, dict):
        return None, None

    date_parts = date_obj.get("date-parts")
    if not date_parts or not isinstance(date_parts, list):
        return None, None

    first_date = date_parts[0]
    if not isinstance(first_date, list) or not first_date:
        return None, None

    try:
        year = int(first_date[0])
        month = int(first_date[1]) if len(first_date) > 1 else 1
        day = int(first_date[2]) if len(first_date) > 2 else 1
    except (TypeError, ValueError):
        return None, None

    if len(first_date) == 1:
        precision = "year"
    elif len(first_date) == 2:
        precision = "month"
    else:
        precision = "day"

    return f"{year:04d}-{month:02d}-{day:02d}", precision


def get_date_time(date_obj: Any) -> str | None:
    """Extract CrossRef date-time if available."""
    if isinstance(date_obj, dict):
        return date_obj.get("date-time")
    return None


def get_resource_url(resource_obj: Any) -> str | None:
    """Extract primary resource URL from nested resource object."""
    if not isinstance(resource_obj, dict):
        return None

    primary = resource_obj.get("primary")
    if not isinstance(primary, dict):
        return None

    return primary.get("URL")

def clean_abstract(value: Any) -> str | None:
    """
    Clean CrossRef abstract text.
    """
    if not value:
        return None

    text = str(value).strip()

    # Remove simple XML/HTML-like tags.
    text = re.sub(r"<[^>]+>", " ", text)

    # Normalize whitespace.
    text = re.sub(r"\s+", " ", text).strip()

    return text or None

def extract_extra_metadata(item: dict[str, Any]) -> dict[str, Any]:
    """
    Keep rare/unmodeled fields instead of losing them.
    """
    return {
        key: value
        for key, value in item.items()
        if key not in KNOWN_FIELDS
    }


def parse_work(item: dict[str, Any]) -> dict[str, Any]:
    """
    Convert one raw CrossRef item into one staged work record.

    This is light flattening:
    - simple analytical fields become columns
    - nested fields are preserved as nested JSON-compatible objects
    - rare fields go to extra_metadata
    """
    published_date, published_date_precision = parse_date_parts(item.get("published"))
    published_print_date, published_print_date_precision = parse_date_parts(item.get("published-print"))
    published_online_date, published_online_date_precision = parse_date_parts(item.get("published-online"))
    issued_date, issued_date_precision = parse_date_parts(item.get("issued"))

    return {
        "doi": normalize_doi(item.get("DOI")),
        "title": first_from_list(item.get("title")),
        "abstract": clean_abstract(item.get("abstract")),
        "publication_type": item.get("type"),
        "publisher": item.get("publisher"),
        "source": item.get("source"),
        "prefix": item.get("prefix"),
        "member": item.get("member"),
        "score": item.get("score"),
        "container_title": first_from_list(item.get("container-title")),
        "short_container_title": first_from_list(item.get("short-container-title")),

        # Keep publication dates.
        "published_date": published_date,
        "published_date_precision": published_date_precision,
        "published_print_date": published_print_date,
        "published_print_date_precision": published_print_date_precision,
        "published_online_date": published_online_date,
        "published_online_date_precision": published_online_date_precision,
        "issued_date": issued_date,
        "issued_date_precision": issued_date_precision,

        # Source/system timestamps.
        "created_date": parse_date_parts(item.get("created"))[0],
        "created_datetime": get_date_time(item.get("created")),
        "deposited_date": parse_date_parts(item.get("deposited"))[0],
        "deposited_datetime": get_date_time(item.get("deposited")),
        "indexed_date": parse_date_parts(item.get("indexed"))[0],
        "indexed_datetime": get_date_time(item.get("indexed")),

        # Counts and basic bibliographic fields.
        "reference_count": item.get("reference-count"),
        "references_count": item.get("references-count"),
        "is_referenced_by_count": item.get("is-referenced-by-count"),
        "volume": item.get("volume"),
        "issue": item.get("issue"),
        "page": item.get("page"),

        # Identifiers and URLs.
        "url": item.get("URL"),
        "resource_url": get_resource_url(item.get("resource")),
        "issn": item.get("ISSN", []),
        "issn_type": item.get("issn-type", []),
        "alternative_ids": item.get("alternative-id", []),
        "isbn": item.get("ISBN", []),
        "isbn_type": item.get("isbn-type", []),
        "language": item.get("language"),
        "article_number": item.get("article-number"),

        # Keep complex nested fields for later modeling.
        "authors": item.get("author", []),
        "license": item.get("license", []),
        "links": item.get("link", []),
        "content_domain": item.get("content-domain"),
        "journal_issue": item.get("journal-issue"),

        # Rare fields are preserved here.
        "extra_metadata": extract_extra_metadata(item),
    }


@log_stage
def create_staging_json(raw_json_path: Path) -> Path:
    """
    Read raw CrossRef JSON and write one staging JSON file.

    Output structure:
    {
        "metadata": {...},
        "works": [...]
    }
    """
    with raw_json_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    items = payload.get("message", {}).get("items", [])

    works = [parse_work(item) for item in items]

    staging_payload = {
        "metadata": {
            "source": "crossref",
            "raw_file": str(raw_json_path),
            "staged_at_utc": utc_now_iso(),
            "work_count": len(works),
        },
        "works": works,
    }

    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    run_name = raw_json_path.stem.replace("crossref_raw_", "")
    output_path = STAGING_DIR / f"stg_crossref_{run_name}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(staging_payload, file, ensure_ascii=False, indent=2)

    logger.info("Saved staging JSON: %s", output_path)
    logger.info("Staged works: %s", len(works))

    return output_path