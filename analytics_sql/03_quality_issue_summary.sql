SELECT
    issue_type,
    COUNT(DISTINCT doi) AS affected_publications,
    SUM(affected_row_count) AS affected_rows

FROM read_parquet('{quality_path}')

GROUP BY issue_type

ORDER BY affected_publications DESC;