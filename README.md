# CrossRef Metadata Pipeline

## Overview

This project is a prototype data engineering pipeline for CrossRef metadata. 
It follows a data lake-style architecture: raw API responses are stored first, then parsed, converted to Parquet, cleaned, validated, and queried for analytics.

```text
Raw JSON -> Staging JSON -> Bronze Parquet -> Silver Parquet -> Analytics queries
```

## Tech Stack

* Python 
* DuckDB 
* Parquet 
* Polars 
* Docker/MinIO for raw object storage

## Setup

```bash
python -m venv .venv
```

```bash
pip install -r requirements.txt
```

## Run

```bash
python run_pipeline.py
```
Generated data is written to `data/`, which is ignored by Git.


## Data Layers

| Layer   | Output          | Purpose                                                       |
| ------- | --------------- | ------------------------------------------------------------- |
| Raw     | `data/raw/`     | Original CrossRef API response                                |
| Staging | `data/staging/` | Parsed JSON with selected fields and preserved extra metadata |
| Bronze  | `data/bronze/`  | Parquet version of staged records                             |
| Silver  | `data/silver/`  | Cleaned, normalized, quality-checked tables                   |

## Silver Tables
* silver_publications: one deduplicated row per DOI
* silver_publication_authors: one row per publication-author relationship
* silver_identifiers: one row per publication-identifier relationship
* silver_quality_issues: detected metadata issues
* silver_validation_report: schema, uniqueness, referential and freshness checks

Some CrossRef author records contain a name field without given or family. These values are preserved as raw_author_name, but excluded from author analytics because they may contain unmapped affiliation-like text.


## Data Quality

The pipeline creates a quality issue table and a validation report. It checks for:

* missing DOI
* missing title
* missing authors
* future publication dates
* unparsed author-name records
* duplicate DOI records in silver
* author rows without matching publication rows

## Analytics

Example DuckDB queries are stored in:

```text
sql/analytics/
```

They include:

1. Author publication statistics
2. Publisher and publication type summary
3. Quality issue summary


## MinIO Raw Storage

The pipeline uploads raw CrossRef JSON files to a local MinIO bucket. 

