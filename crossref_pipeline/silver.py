import logging
from pathlib import Path

import duckdb

from crossref_pipeline.paths import SILVER_DIR
from crossref_pipeline.utils.logging_utils import log_stage


logger = logging.getLogger(__name__)


@log_stage
def create_silver_tables(bronze_parquet_path: Path) -> dict[str, Path]:
    """
    Create silver-layer tables from bronze CrossRef works.
    - cast important fields to useful types
    - deduplicate publication records by DOI
    - normalize nested authors into a separate table
    - create simple data-quality issue table

    Intentionally omitted from silver_publications:
    - created_date/deposited_date/indexed_date: redundant because corresponding datetime fields are kept
    - content_domain_json: CrossRef domain/Crossmark metadata, preserved in bronze only
    - journal_issue_json: nested issue metadata, preserved in bronze
    - links_json: preserved in bronze
    - score: CrossRef API response/search score, not part of the core publication model
    """
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    run_name = bronze_parquet_path.stem.replace("bronze_crossref_", "")

    publications_path = SILVER_DIR / f"silver_publications_{run_name}.parquet"
    authors_path = SILVER_DIR / f"silver_publication_authors_{run_name}.parquet"
    identifiers_path = SILVER_DIR / f"silver_publication_identifiers_{run_name}.parquet"
    quality_path = SILVER_DIR / f"silver_quality_issues_{run_name}.parquet"

    bronze_path_sql = bronze_parquet_path.as_posix()
    publications_path_sql = publications_path.as_posix()
    authors_path_sql = authors_path.as_posix()
    identifiers_path_sql = identifiers_path.as_posix()
    quality_path_sql = quality_path.as_posix()

    con = duckdb.connect()

    # ------------------------------------------------------------------
    # 1. Silver publications
    # ------------------------------------------------------------------
    # One row per DOI/publication.
    # Dates are cast from text to DATE/TIMESTAMP where possible.
    # Deduplication is kept even if current sample has unique DOIs.
    con.execute(
        f"""
        COPY (
            WITH ranked AS (
                SELECT
                    doi,
                    title,
                    abstract,
                    publication_type,
                    publisher,
                    language,
                    article_number,
                    license_json,
                    extra_metadata_json,
                    source,
                    prefix,
                    member,
                    container_title,
                    short_container_title,

                    TRY_CAST(published_date AS DATE) AS published_date,
                    published_date_precision,
                    TRY_CAST(published_print_date AS DATE) AS published_print_date,
                    published_print_date_precision,
                    TRY_CAST(published_online_date AS DATE) AS published_online_date,
                    published_online_date_precision,
                    TRY_CAST(issued_date AS DATE) AS issued_date,
                    issued_date_precision,

                    TRY_CAST(created_datetime AS TIMESTAMP) AS created_datetime,
                    TRY_CAST(deposited_datetime AS TIMESTAMP) AS deposited_datetime,
                    TRY_CAST(indexed_datetime AS TIMESTAMP) AS indexed_datetime,

                    TRY_CAST(reference_count AS INTEGER) AS reference_count,
                    TRY_CAST(references_count AS INTEGER) AS references_count,
                    TRY_CAST(is_referenced_by_count AS INTEGER) AS is_referenced_by_count,

                    volume,
                    issue,
                    page,
                    url,
                    resource_url,

                    ROW_NUMBER() OVER (
                        PARTITION BY doi
                        ORDER BY
                            TRY_CAST(deposited_datetime AS TIMESTAMP) DESC NULLS LAST,
                            TRY_CAST(indexed_datetime AS TIMESTAMP) DESC NULLS LAST
                    ) AS row_number

                FROM read_parquet('{bronze_path_sql}')
                WHERE doi IS NOT NULL
            )

            SELECT
                doi,
                title,
                abstract,
                publication_type,
                publisher,
                language,
                article_number,
                license_json,
                extra_metadata_json,
                source,
                prefix,
                member,
                container_title,
                short_container_title,
                published_date,
                published_date_precision,
                published_print_date,
                published_print_date_precision,
                published_online_date,
                published_online_date_precision,
                issued_date,
                issued_date_precision,
                created_datetime,
                deposited_datetime,
                indexed_datetime,
                reference_count,
                references_count,
                is_referenced_by_count,
                volume,
                issue,
                page,
                url,
                resource_url
            FROM ranked
            WHERE row_number = 1
        )
        TO '{publications_path_sql}'
        (FORMAT PARQUET);
        """
    )

    # ------------------------------------------------------------------
    # 2. Silver publication authors
    # ------------------------------------------------------------------
    # authors_json is a nested JSON array in bronze.
    # json_each turns each author into one row.
    con.execute(
        f"""
        COPY (
            SELECT
                bronze.doi,
                json_extract_string(author.value, '$.given') AS given_name,
                json_extract_string(author.value, '$.family') AS family_name,
                json_extract_string(author.value, '$.name') AS raw_author_name,
                REPLACE(
                    REPLACE(
                        json_extract_string(author.value, '$.ORCID'),
                        'https://orcid.org/',
                        ''
                    ),
                    'http://orcid.org/',
                    ''
                ) AS orcid,
                json_extract_string(author.value, '$.sequence') AS sequence,
                CAST(to_json(json_extract(author.value, '$.affiliation')) AS VARCHAR) AS affiliations_json,
                
                CASE
                    WHEN json_extract_string(author.value, '$.given') IS NOT NULL
                      OR json_extract_string(author.value, '$.family') IS NOT NULL
                    THEN 'parsed_person_name'
            
                    WHEN json_extract_string(author.value, '$.name') IS NOT NULL
                    THEN 'name_only_unparsed'
            
                    ELSE 'missing_author_name'
                END AS author_parse_status      
                
            FROM read_parquet('{bronze_path_sql}') AS bronze,
                 json_each(bronze.authors_json) AS author
            WHERE bronze.doi IS NOT NULL
        )
        TO '{authors_path_sql}'
        (FORMAT PARQUET);
        """
    )


    # ------------------------------------------------------------------
    # 3. Silver publication identifiers
    # ------------------------------------------------------------------
    # Publication identifiers are normalized into a separate table.
    # json_each explodes each JSON array so every ISSN, ISBN, or alternative ID
    # becomes one row linked back to the publication DOI.
    con.execute(f"""
    COPY (
        WITH identifier_rows AS (

            SELECT
                doi,
                'ISSN' AS identifier_type,
                json_extract_string(identifier.value, '$.value') AS identifier_value,
                json_extract_string(identifier.value, '$.type') AS identifier_subtype,
                CAST(identifier.key AS INTEGER) + 1 AS identifier_position
            FROM read_parquet('{bronze_path_sql}') AS bronze,
                 json_each(COALESCE(bronze.issn_type_json, '[]')) AS identifier

            UNION ALL

            SELECT
                doi,
                'ISBN' AS identifier_type,
                json_extract_string(identifier.value, '$.value') AS identifier_value,
                json_extract_string(identifier.value, '$.type') AS identifier_subtype,
                CAST(identifier.key AS INTEGER) + 1 AS identifier_position
            FROM read_parquet('{bronze_path_sql}') AS bronze,
                 json_each(COALESCE(bronze.isbn_type_json, '[]')) AS identifier

            UNION ALL

            SELECT
                doi,
                'alternative_id' AS identifier_type,
                json_extract_string(identifier.value, '$') AS identifier_value,
                NULL AS identifier_subtype,
                CAST(identifier.key AS INTEGER) + 1 AS identifier_position
            FROM read_parquet('{bronze_path_sql}') AS bronze,
                 json_each(COALESCE(bronze.alternative_ids_json, '[]')) AS identifier
        )

        SELECT *
        FROM identifier_rows
        WHERE identifier_value IS NOT NULL
          AND TRIM(identifier_value) != ''
    )
    TO '{identifiers_path_sql}'
    (FORMAT PARQUET);
    """)


    # ------------------------------------------------------------------
    # 4. Silver quality issues
    # ------------------------------------------------------------------
    # This creates one row per detected issue.
    con.execute(
        f"""
        COPY (
            SELECT
                doi,
                'missing_title' AS issue_type,
                'Publication has no title' AS issue_description,
            1 AS affected_row_count
            FROM read_parquet('{bronze_path_sql}')
            WHERE title IS NULL

            UNION ALL

            SELECT
                doi,
                'missing_authors' AS issue_type,
                'Publication has no authors' AS issue_description,
            1 AS affected_row_count
            FROM read_parquet('{bronze_path_sql}')
            WHERE authors_json IS NULL
               OR authors_json = '[]'

            UNION ALL

            SELECT
                doi,
                'future_published_date' AS issue_type,
                'Published date is later than current date' AS issue_description,
            1 AS affected_row_count
            FROM read_parquet('{bronze_path_sql}')
            WHERE TRY_CAST(published_date AS DATE) > CURRENT_DATE

            UNION ALL

            SELECT
                doi,
                'missing_doi' AS issue_type,
                'Publication has no DOI' AS issue_description,
            1 AS affected_row_count
            FROM read_parquet('{bronze_path_sql}')
            WHERE doi IS NULL
            
            UNION ALL

            SELECT
                bronze.doi,
                'author_name_only_unparsed' AS issue_type,
                'Author record has name field but no given/family fields; value is preserved but not used for author analytics as it may contain unmapped affiliation text' AS issue_description,
                COUNT(*) AS affected_row_count
            FROM read_parquet('{bronze_path_sql}') AS bronze,
                 json_each(bronze.authors_json) AS author
            WHERE json_extract_string(author.value, '$.name') IS NOT NULL
              AND json_extract_string(author.value, '$.given') IS NULL
              AND json_extract_string(author.value, '$.family') IS NULL
            GROUP BY bronze.doi
  
        )
        TO '{quality_path_sql}'
        (FORMAT PARQUET);
        """
    )

    con.close()

    logger.info("Saved silver publications: %s", publications_path)
    logger.info("Saved silver publication authors: %s", authors_path)
    logger.info("Saved silver publication identifiers: %s", identifiers_path)
    logger.info("Saved silver quality issues: %s", quality_path)

    return {
        "publications": publications_path,
        "publication_authors": authors_path,
        "identifiers": identifiers_path,
        "quality_issues": quality_path,
    }