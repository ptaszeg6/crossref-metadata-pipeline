WITH structured_authors AS (
    SELECT
        doi,
        sequence,
        NULLIF(TRIM(CONCAT_WS(' ', given_name, family_name)), '') AS author_name
    FROM read_parquet('{authors_path}')
    WHERE given_name IS NOT NULL
       OR family_name IS NOT NULL
)

SELECT
    author_name,

    COUNT(DISTINCT doi) AS publication_count,

    SUM(
        CASE
            WHEN sequence = 'first' THEN 1
            ELSE 0
        END
    ) AS first_author_count

FROM structured_authors

WHERE author_name IS NOT NULL

GROUP BY author_name

ORDER BY
    publication_count DESC,
    first_author_count DESC

LIMIT 20;