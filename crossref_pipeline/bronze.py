import logging
from pathlib import Path

import duckdb

from crossref_pipeline.paths import BRONZE_DIR
from crossref_pipeline.utils.logging_utils import log_stage


logger = logging.getLogger(__name__)


@log_stage
def create_bronze_parquet(staging_json_path: Path) -> Path:
    """
    Convert staging JSON into bronze Parquet using DuckDB.

    The staging JSON contains one top-level object with a "works" array.
    DuckDB reads the JSON, unnests the works array, and writes a Parquet file.
    """
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    run_name = staging_json_path.stem.replace("stg_crossref_", "")
    output_path = BRONZE_DIR / f"bronze_crossref_{run_name}.parquet"

    staging_path_sql = staging_json_path.as_posix()
    output_path_sql = output_path.as_posix()

    con = duckdb.connect()

    con.execute(
        f"""
        COPY (
            SELECT
                work.doi AS doi,
                work.title AS title,
                work.abstract AS abstract,
                work.publication_type AS publication_type,
                work.publisher AS publisher,
                work.source AS source,
                work.prefix AS prefix,
                work.member AS member,
                work.score AS score,
                work.container_title AS container_title,
                work.short_container_title AS short_container_title,

                work.published_date AS published_date,
                work.published_print_date AS published_print_date,
                work.published_online_date AS published_online_date,
                work.issued_date AS issued_date,
                work.published_date_precision AS published_date_precision,
                work.published_print_date_precision AS published_print_date_precision,
                work.published_online_date_precision AS published_online_date_precision,
                work.issued_date_precision AS issued_date_precision,
                work.created_date AS created_date,
                work.created_datetime AS created_datetime,
                work.deposited_date AS deposited_date,
                work.deposited_datetime AS deposited_datetime,
                work.indexed_date AS indexed_date,
                work.indexed_datetime AS indexed_datetime,

                work.reference_count AS reference_count,
                work.references_count AS references_count,
                work.is_referenced_by_count AS is_referenced_by_count,
                work.volume AS volume,
                work.issue AS issue,
                work.page AS page,

                work.url AS url,
                work.resource_url AS resource_url,

                CAST(to_json(work.issn) AS VARCHAR) AS issn_json,
                CAST(to_json(work.issn_type) AS VARCHAR) AS issn_type_json,
                CAST(to_json(work.authors) AS VARCHAR) AS authors_json,
                CAST(to_json(work.license) AS VARCHAR) AS license_json,
                CAST(to_json(work.links) AS VARCHAR) AS links_json,
                CAST(to_json(work.content_domain) AS VARCHAR) AS content_domain_json,
                CAST(to_json(work.journal_issue) AS VARCHAR) AS journal_issue_json,
                CAST(to_json(work.alternative_ids) AS VARCHAR) AS alternative_ids_json,
                CAST(to_json(work.isbn) AS VARCHAR) AS isbn_json,
                CAST(to_json(work.isbn_type) AS VARCHAR) AS isbn_type_json,
                work.language AS language,
                work.article_number AS article_number,
                CAST(to_json(work.extra_metadata) AS VARCHAR) AS extra_metadata_json

            FROM (
                SELECT unnest(works) AS work
                FROM read_json_auto('{staging_path_sql}')
            )
        )
        TO '{output_path_sql}'
        (FORMAT PARQUET);
        """
    )

    con.close()

    logger.info("Saved bronze Parquet: %s", output_path)

    return output_path