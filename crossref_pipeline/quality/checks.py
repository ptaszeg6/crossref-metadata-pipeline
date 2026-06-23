import logging
from pathlib import Path

import duckdb

from crossref_pipeline.paths import SILVER_DIR
from crossref_pipeline.utils.logging_utils import log_stage


logger = logging.getLogger(__name__)


def get_columns(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> set[str]:
    """Return column names from a Parquet file."""
    path_sql = parquet_path.as_posix()

    rows = con.execute(
        f"""
        DESCRIBE SELECT *
        FROM read_parquet('{path_sql}')
        """
    ).fetchall()

    return {row[0] for row in rows}

def add_check(
    checks: list[tuple[str, str, str | None, str]],
    check_name: str,
    passed: bool,
    metric_value: str | None,
    details: str,
) -> None:
    """Add one validation check result to the checks list."""
    checks.append(
        (
            check_name,
            "pass" if passed else "fail",
            metric_value,
            details,
        )
    )

@log_stage
def create_silver_validation_report(silver_paths: dict[str, Path]) -> Path:
    """
    Create a small validation report for the silver layer.

    This covers:
    - schema checks
    - uniqueness checks
    - referential checks
    - quality issue counts
    - freshness indicators
    """
    publications_path = silver_paths["publications"]
    authors_path = silver_paths["publication_authors"]
    identifiers_path = silver_paths["identifiers"]
    quality_path = silver_paths["quality_issues"]

    run_name = publications_path.stem.replace("silver_publications_", "")
    output_path = SILVER_DIR / f"silver_validation_report_{run_name}.parquet"

    con = duckdb.connect()

    publication_columns = get_columns(con, publications_path)
    author_columns = get_columns(con, authors_path)
    identifier_columns = get_columns(con, identifiers_path)
    quality_columns = get_columns(con, quality_path)

    checks: list[tuple[str, str, str | None, str]] = []

    # ------------------------------------------------------------------
    # Schema checks
    # ------------------------------------------------------------------
    required_publication_columns = {
        "doi",
        "title",
        "abstract",
        "publication_type",
        "publisher",
        "language",
        "article_number",
        "license_json",
        "extra_metadata_json",
        "source",
        "prefix",
        "member",
        "container_title",
        "short_container_title",
        "published_date",
        "published_date_precision",
        "published_print_date",
        "published_print_date_precision",
        "published_online_date",
        "published_online_date_precision",
        "issued_date",
        "issued_date_precision",
        "created_datetime",
        "deposited_datetime",
        "indexed_datetime",
        "reference_count",
        "references_count",
        "is_referenced_by_count",
        "volume",
        "issue",
        "page",
        "url",
        "resource_url"
    }

    required_author_columns = {
        "doi",
        "given_name",
        "family_name",
        "raw_author_name",
        "orcid",
        "sequence",
        "affiliations_json",
        "author_parse_status",
    }

    required_identifier_columns = {
        "doi",
        "identifier_type",
        "identifier_value",
        "identifier_subtype",
        "identifier_position",
    }

    required_quality_columns = {
        "doi",
        "issue_type",
        "issue_description",
        "affected_row_count",
    }

    add_check(
        checks,
        "schema_publications_required_columns",
        required_publication_columns.issubset(publication_columns),
        str(sorted(required_publication_columns - publication_columns)),
        "silver_publications should contain required analytical columns",
    )

    add_check(
        checks,
        "schema_authors_required_columns",
        required_author_columns.issubset(author_columns),
        str(sorted(required_author_columns - author_columns)),
        "silver_publication_authors should contain required author columns",
    )

    add_check(
        checks,
        "schema_identifiers_required_columns",
        required_identifier_columns.issubset(identifier_columns),
        str(sorted(required_identifier_columns - identifier_columns)),
        "silver_publication_identifiers should contain required identifier columns",
    )

    add_check(
        checks,
        "schema_quality_required_columns",
        required_quality_columns.issubset(quality_columns),
        str(sorted(required_quality_columns - quality_columns)),
        "silver_quality_issues should contain required quality columns",
    )

    # ------------------------------------------------------------------
    # Data checks
    # ------------------------------------------------------------------
    publications_sql = publications_path.as_posix()
    authors_sql = authors_path.as_posix()
    identifiers_sql = identifiers_path.as_posix()
    quality_sql = quality_path.as_posix()

    publication_count, distinct_doi_count = con.execute(
        f"""
        SELECT
            COUNT(*) AS publication_count,
            COUNT(DISTINCT doi) AS distinct_doi_count
        FROM read_parquet('{publications_sql}')
        """
    ).fetchone()

    add_check(
        checks,
        "unique_publication_doi",
        publication_count == distinct_doi_count,
        f"rows={publication_count}, distinct_dois={distinct_doi_count}",
        "silver_publications should have one row per DOI",
    )

    missing_publication_doi_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{publications_sql}')
        WHERE doi IS NULL
        """
    ).fetchone()[0]

    add_check(
        checks,
        "no_missing_publication_doi",
        missing_publication_doi_count == 0,
        str(missing_publication_doi_count),
        "silver_publications should not contain missing DOI values",
    )

    author_without_publication_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{authors_sql}') AS authors
        LEFT JOIN read_parquet('{publications_sql}') AS publications
            ON authors.doi = publications.doi
        WHERE publications.doi IS NULL
        """
    ).fetchone()[0]

    add_check(
        checks,
        "authors_have_matching_publication",
        author_without_publication_count == 0,
        str(author_without_publication_count),
        "Every author row should match a DOI in silver_publications",
    )

    identifier_without_publication_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{identifiers_sql}') AS identifiers
        LEFT JOIN read_parquet('{publications_sql}') AS publications
            ON identifiers.doi = publications.doi
        WHERE publications.doi IS NULL
        """
    ).fetchone()[0]

    add_check(
        checks,
        "identifiers_have_matching_publication",
        identifier_without_publication_count == 0,
        str(identifier_without_publication_count),
        "Every identifier row should match a DOI in silver_publications",
    )

    # ------------------------------------------------------------------
    # Freshness / observability indicators
    # ------------------------------------------------------------------
    max_indexed_datetime = con.execute(
        f"""
        SELECT MAX(indexed_datetime)
        FROM read_parquet('{publications_sql}')
        """
    ).fetchone()[0]

    checks.append(
        (
            "freshness_max_indexed_datetime",
            "info",
            str(max_indexed_datetime),
            "Latest indexed_datetime found in silver_publications",
        )
    )

    quality_issue_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{quality_sql}')
        """
    ).fetchone()[0]

    checks.append(
        (
            "quality_issue_count",
            "info",
            str(quality_issue_count),
            "Number of quality issue records generated in silver_quality_issues",
        )
    )

    # ------------------------------------------------------------------
    # Save report
    # ------------------------------------------------------------------
    con.execute(
        """
        CREATE TEMP TABLE validation_report (
            check_name VARCHAR,
            status VARCHAR,
            metric_value VARCHAR,
            details VARCHAR
        )
        """
    )

    con.executemany(
        """
        INSERT INTO validation_report
        VALUES (?, ?, ?, ?)
        """,
        checks,
    )

    con.execute(
        f"""
        COPY (
            SELECT
                CURRENT_TIMESTAMP AS checked_at,
                *
            FROM validation_report
        )
        TO '{output_path.as_posix()}'
        (FORMAT PARQUET)
        """
    )

    failed_checks = [
        check
        for check in checks
        if check[1] == "fail"
    ]

    con.close()

    logger.info("Saved silver validation report: %s", output_path)

    if failed_checks:
        logger.warning("Silver validation completed with failed checks: %s", failed_checks)

    return output_path