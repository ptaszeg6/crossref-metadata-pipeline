SELECT
    publisher,
    publication_type,
    COUNT(*) AS publication_count,
    AVG(is_referenced_by_count) AS avg_citation_count

FROM read_parquet('{publications_path}')

GROUP BY publisher, publication_type

ORDER BY publication_count DESC
LIMIT 20;