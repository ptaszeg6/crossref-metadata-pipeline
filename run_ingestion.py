from crossref_pipeline.ingest import run_raw_ingestion
from crossref_pipeline.paths import create_data_dirs
from crossref_pipeline.staging import create_staging_json
from crossref_pipeline.utils.logging_utils import setup_logging
from crossref_pipeline.bronze import create_bronze_parquet
from crossref_pipeline.silver import create_silver_tables
from crossref_pipeline.quality.checks import create_silver_validation_report
from crossref_pipeline.storage.minio_client import upload_raw_file_to_minio
import logging
from crossref_pipeline.utils.logging_utils import log_stage

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    create_data_dirs()

    raw_json_path = run_raw_ingestion(rows=200)
    staging_json_path = create_staging_json(raw_json_path)
    bronze_parquet_path = create_bronze_parquet(staging_json_path)
    silver_paths = create_silver_tables(bronze_parquet_path)
    create_silver_validation_report(silver_paths)

    try:
        upload_raw_file_to_minio(raw_json_path)
    except Exception as error:
        logger.warning("Skipping MinIO upload: %s", error)

if __name__ == "__main__":
    main()