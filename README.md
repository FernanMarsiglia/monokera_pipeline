# SpaceNews Data Pipeline

<div align="center">

**Production-grade data pipeline for SpaceNews API ingestion, enrichment, and analytics**

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Redshift-orange.svg)](https://aws.amazon.com/)
[![Airflow](https://img.shields.io/badge/Airflow-3.0.4-green.svg)](https://airflow.apache.org/)

</div>

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [AWS Deployment](#aws-deployment)
- [Testing](#testing)
- [Monitoring](#monitoring)
- [Documentation](#documentation)

---

## 🌟 Overview

The **SpaceNews Pipeline** is a complete end-to-end data engineering solution that:

1. **Extracts** space industry content from the [SpaceNews API](https://api.spaceflightnewsapi.net/v4/)
2. **Validates** and **cleanses** raw data with quarantine handling
3. **Enriches** content with topics, entities, and keywords
4. **Aggregates** trends and metrics
5. **Loads** to AWS Redshift for analytics
6. **Monitors** pipeline health with SNS alerts

### Data Sources
- **Articles**: Space industry news articles
- **Blogs**: Space blogs and opinion pieces
- **Reports**: Industry reports and whitepapers
- **Info**: API metadata (news sites, version)

---

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────┐
│  SpaceNews API  │
└────────┬────────┘
         │ Extract
         ▼
┌─────────────────┐
│ Bronze (S3)     │  🗂️  Immutable JSONL, append-only
│ data/bronze/    │      Partitioned by ingest_date
└────────┬────────┘
         │ Clean & Deduplicate (Glue Job 01)
         ▼
┌─────────────────┐
│ Silver (S3)     │  ✨  Validated Parquet
│ data/silver/    │      Deduplicated, clean timestamps
│ data/silver/    │      Invalid → quarantine/
│   quarantine/   │
└────────┬────────┘
         │ Enrich (Glue Job 02)
         ▼
┌─────────────────┐
│ Gold (S3)       │  🏆  Enriched Parquet
│ data/gold/      │      + keywords, entities, topics
│   content_      │      + JSON arrays for relations
│   enriched/     │
│   trends/       │      Aggregations (Glue Job 03)
└────────┬────────┘
         │ Load (Redshift COPY + MERGE)
         ▼
┌─────────────────┐
│ Redshift        │  🗄️  Analytics warehouse
│                 │
│ **stg schema**  │      Staging (temporary)
│ - stg_content_  │
│   items         │
│                 │
│ **core schema** │      All business tables
│ - content_items │      → Main content (SCD Type 2)
│                 │
│ - ref_news_     │      → Reference dimensions
│   sites         │
│ - ref_topics    │
│                 │
│ - agg_topic_    │      → Pre-aggregated metrics
│   monthly_      │
│   trends        │
│ - agg_news_site_│
│   monthly_      │
│   activity      │
│                 │
│ - meta_pipeline_│      → Pipeline metadata
│   runs          │
│ - meta_api_info_│
│   runs          │
│ - meta_data_    │
│   quality_      │
│   checks        │
└─────────────────┘
```

### Tech Stack

| Layer | Technology |
|-------|-----------|
| **Orchestration** | Apache Airflow 3.0.4 (Docker) |
| **Ingestion** | Python + Requests + Click CLI |
| **Processing** | AWS Glue 4.0 (Spark 3.3.0, Python 3.10) |
| **Storage** | AWS S3 (JSONL, Parquet) |
| **Warehouse** | AWS Redshift Serverless |
| **Alerts** | AWS SNS |
| **Testing** | pytest, moto |

---

## ✨ Features

### Production-Grade Ingestion
- ✅ **Pagination**: Follows API `next` links until null
- ✅ **Rate Limiting**: Token bucket algorithm (10 req/sec)
- ✅ **Retries**: Exponential backoff with jitter
- ✅ **Validation**: Schema checks, required fields, quarantine invalid
- ✅ **Structured Logging**: JSON logs with correlation_id, run_id
- ✅ **Alerting**: SNS notifications on failures/successes

### Data Quality
- ✅ **Deduplication**: By (content_type, content_id), keep latest
- ✅ **Quarantine**: Invalid records isolated with error_reason
- ✅ **Hashing**: MD5 record_hash for change detection
- ✅ **SCD Type 2**: Full history tracking in Redshift

### Enrichments
- ✅ **Keywords**: Extracted from title + summary
- ✅ **Entities**: SpaceX, NASA, ESA, JAXA, Blue Origin, etc.
- ✅ **Topics**: Launches, Mars, Moon, Satellites, ISS, etc.
- ✅ **Temporal**: published_year/month/day for partitioning

### Aggregations
- ✅ **Topic Trends**: Monthly content counts by topic
- ✅ **Source Activity**: Monthly activity by news_site

---

## 📦 Requirements

### Local Development
- **Python**: 3.11+
- **Docker**: For Airflow (docker-compose)
- **Make**: Optional, for convenience commands
- **AWS CLI**: Configured with credentials

### AWS Resources (Required)
- **S3 Bucket**: `monokera-bucket` (or custom name)
- **Glue Jobs**: 3 jobs (01-clean, 02-enrich, 03-trends)
- **Redshift Serverless**: Workgroup + database
- **SNS Topic**: For pipeline alerts
- **IAM Roles**: For Glue, Redshift access to S3

---

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/your-org/monokera-pipeline.git
cd monokera-pipeline

# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 2. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your values
nano .env
```

**Required variables:**
```ini
# AWS
AWS_REGION=us-east-1
AWS_PROFILE=default
S3_BUCKET=monokera-bucket
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:311048569989:spacenews-pipeline-alerts

# Redshift
REDSHIFT_HOST=monokera-pipeline.311048569989.us-east-1.redshift-serverless.amazonaws.com
REDSHIFT_DATABASE=dev
REDSHIFT_USER=admin
REDSHIFT_PASSWORD=your_password
REDSHIFT_IAM_ROLE=arn:aws:iam::311048569989:role/RedshiftS3ReadRole

# API
SPACENEWS_API_BASE_URL=https://api.spaceflightnewsapi.net/v4
RATE_LIMIT_PER_SECOND=10
```

### 3. Run Extraction (Local)

```bash
# Extract articles for today
python -m spacenews_ingestion.cli extract \
  --endpoint articles \
  --ingest-date 2026-02-17 \
  --limit 100

# Validate extraction
python -m spacenews_ingestion.cli validate-extraction --ingest-date 2026-02-17

# List recent extractions
python -m spacenews_ingestion.cli list-extractions
```

### 4. Start Airflow (Docker)

```bash
# Start Airflow services
make airflow-up

# Access UI: http://localhost:8080
# Username: airflow
# Password: airflow
```

---

## 📖 Usage

### CLI Commands

#### Extract Data
```bash
# Extract all articles (until next=null)
python -m spacenews_ingestion.cli extract --endpoint articles --ingest-date 2026-02-17

# Extract with limit
python -m spacenews_ingestion.cli extract --endpoint blogs --ingest-date 2026-02-17 --limit 500

# Extract with date filter
python -m spacenews_ingestion.cli extract --endpoint reports --ingest-date 2026-02-17 --since 2026-02-01

# Dry-run (no S3 upload, no alerts)
python -m spacenews_ingestion.cli extract --endpoint articles --ingest-date 2026-02-17 --dry-run
```

#### Validate Data
```bash
# Validate bronze data for a date
python -m spacenews_ingestion.cli validate-extraction --ingest-date 2026-02-17

# Check all endpoints
python -m spacenews_ingestion.cli validate-extraction --ingest-date 2026-02-17 --endpoint all
```

#### List Extractions
```bash
# List recent extraction runs
python -m spacenews_ingestion.cli list-extractions
```

### Glue Jobs (Run via Airflow or AWS Console)

```bash
# Job 01: Bronze → Silver (Clean & Deduplicate)
aws glue start-job-run \
  --job-name spacenews-01-clean-dedup \
  --arguments='{"--S3_BUCKET":"monokera-bucket","--INGEST_DATE":"2026-02-17"}'

# Job 02: Silver → Gold (Enrich)
aws glue start-job-run \
  --job-name spacenews-02-enrich-topics-entities \
  --arguments='{"--S3_BUCKET":"monokera-bucket","--INGEST_DATE":"2026-02-17"}'

# Job 03: Gold → Gold Trends (Aggregate)
aws glue start-job-run \
  --job-name spacenews-03-trends-aggregations \
  --arguments='{"--S3_BUCKET":"monokera-bucket","--INGEST_DATE":"2026-02-17"}'
```

### Redshift Queries

```sql
-- Run DDL (first time only)
\i sql/redshift_ddl.sql

-- Load data from S3 (replace {{ ingest_date }} and {{ redshift_iam_role }})
\i sql/redshift_load.sql

-- Run analysis queries
\i sql/analysis_queries.sql
```

---

## ☁️ AWS Deployment

### Step 1: Create S3 Bucket

```bash
aws s3 mb s3://monokera-bucket --region us-east-1

# Create folder structure
aws s3api put-object --bucket monokera-bucket --key data/bronze/_meta/
aws s3api put-object --bucket monokera-bucket --key data/silver/quarantine/
aws s3api put-object --bucket monokera-bucket --key glue/scripts/
```

### Step 2: Upload Glue Scripts

```bash
# Upload all 3 Glue jobs
aws s3 cp glue/scripts/01_clean_deduplicate_NEW.py \
  s3://monokera-bucket/glue/scripts/01_clean_deduplicate.py

aws s3 cp glue/scripts/02_enrich_topics_entities_NEW.py \
  s3://monokera-bucket/glue/scripts/02_enrich_topics_entities.py

aws s3 cp glue/scripts/03_trends_aggregations_NEW.py \
  s3://monokera-bucket/glue/scripts/03_trends_aggregations.py

# Upload spark_utils
aws s3 cp glue/scripts/spark_utils.py \
  s3://monokera-bucket/glue/scripts/spark_utils.py
```

### Step 3: Create Glue Jobs (via AWS Console or CLI)

**Job 01: Clean & Deduplicate**
- Name: `spacenews-01-clean-dedup`
- Glue Version: 4.0
- Language: Python 3
- Script location: `s3://monokera-bucket/glue/scripts/01_clean_deduplicate.py`
- Dependent JARs: None
- Python library path: `s3://monokera-bucket/glue/scripts/`
- Job parameters:
  - `--S3_BUCKET`: monokera-bucket
  - `--INGEST_DATE`: (passed at runtime)

Repeat for Job 02 and Job 03.

### Step 4: Configure SNS Topic

**Note**: If you already have an SNS topic and subscription, update `.env` with your ARN:
```
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:311048569989:spacenews-pipeline-alerts
```

**Or create a new one:**
```bash
# Create topic
aws sns create-topic --name spacenews-pipeline-alerts --region us-east-1

# Subscribe email
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:311048569989:spacenews-pipeline-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com

# Confirm subscription from email
```

### Step 5: Create Redshift Schemas & Tables

```bash
# Connect to Redshift
psql -h monokera-pipeline.311048569989.us-east-1.redshift-serverless.amazonaws.com \
     -U admin -d dev -p 5439

# Run DDL to create schemas (stg, core) and all tables
\i sql/redshift_ddl.sql

# Verify schemas
\dn

# Verify tables in stg
\dt stg.*

# Verify tables in core (with prefixes: ref_, rel_, agg_, meta_)
\dt core.*

# Exit
\q
```

### Step 6: Deploy Airflow DAG

```bash
# Copy DAG to Airflow dags/ folder
# If using AWS MWAA:
aws s3 cp dags/spacenews_pipeline.py s3://your-mwaa-bucket/dags/

# Trigger DAG
airflow dags trigger spacenews_pipeline --conf '{"ingest_date":"2026-02-17"}'
```

---

## 🧪 Testing

### Code Quality Checks

```bash
# Run ALL validations (ruff check, ruff format, mypy, pytest)
./check.sh
```

El script `check.sh` ejecuta secuencialmente:
1. **ruff check** - Linting de código
2. **ruff format --check** - Verificación de formato
3. **mypy** - Type checking
4. **pytest** - Tests con reporte de cobertura

### Run All Tests

```bash
# Run pytest
pytest

# With coverage
pytest --cov=src --cov-report=html

# Specific test file
pytest tests/test_paginator.py -v
```

### Test Structure

```
tests/
├── conftest.py                 # pytest fixtures
├── test_api_client.py          # Rate limiting, retries
├── test_paginator.py           # Pagination logic
├── test_validation.py          # Schema validation, quarantine
├── test_checkpoint.py          # Checkpoint management
├── test_extractors.py          # End-to-end extraction
└── test_dag_functions.py       # DAG functions
```

### Code Quality Tools

```bash
./check.sh    # Script único que ejecuta todas las validaciones
```

Para CI/CD, el script retorna exit code 1 si alguna validación falla.

---

## 📊 Monitoring

### Airflow UI
- **URL**: http://localhost:8080 (local) or AWS MWAA console
- **DAG Graph**: Visual pipeline flow
- **Task Logs**: Detailed execution logs
- **Task Duration**: Performance tracking

### CloudWatch Logs (AWS Glue)
```bash
# View Glue job logs
aws logs tail /aws-glue/jobs/output --follow
```

### SNS Alerts

Alert types:
- ❌ **Extraction Failure**: API errors, network issues
- ❌ **Glue Job Failure**: Processing errors
- ⚠️ **Data Quality Issues**: High invalid record percentage
- ⚠️ **Threshold Breaches**: Unexpected data volumes
- ✅ **Pipeline Success**: Daily run completion

### Redshift Queries

```sql
-- Check pipeline run status
SELECT * FROM core.meta_pipeline_runs
ORDER BY started_at DESC LIMIT 10;

-- Check data quality
SELECT * FROM core.meta_data_quality_checks
WHERE check_result != 'pass'
ORDER BY checked_at DESC;

-- Check data freshness
SELECT 
    content_type,
    MAX(published_at) AS latest_published,
    DATEDIFF(hour, MAX(published_at), CURRENT_TIMESTAMP) AS hours_old
FROM core.content_items
WHERE is_current = TRUE
GROUP BY content_type;
```

---

## 📚 Documentation

- **Architecture**: [NEW_ARCHITECTURE.md](NEW_ARCHITECTURE.md) - Complete system design
- **Technical Docs**: [docs/technical_document.md](docs/technical_document.md)
- **Testing Guide**: [TESTING.md](TESTING.md)
- **Data Contracts**: [docs/data_contracts.md](docs/data_contracts.md) *(to be created)*
- **Runbook**: [docs/runbook.md](docs/runbook.md) *(to be created)*

---

## 🛠️ Development

### Project Structure

```
monokera-pipeline/
├── dags/
│   ├── spacenews_pipeline.py          # Production DAG (AWS)
│   └── spacenews_pipeline_local.py    # Local development DAG
├── glue/scripts/
│   ├── 01_clean_deduplicate_NEW.py    # Bronze → Silver
│   ├── 02_enrich_topics_entities_NEW.py  # Silver → Gold
│   ├── 03_trends_aggregations_NEW.py  # Gold → Trends
│   └── spark_utils.py                 # Shared Spark utilities
├── src/spacenews_ingestion/
│   ├── __init__.py
│   ├── api_client.py                  # API interaction
│   ├── paginator.py                   # Pagination logic
│   ├── validation.py                  # Schema validation
│   ├── alerts.py                      # SNS alerting
│   ├── logging_config.py              # Structured logging
│   └── cli.py                         # CLI commands
├── sql/
│   ├── redshift_ddl.sql               # Table definitions
│   ├── redshift_load.sql              # COPY + MERGE logic
│   └── analysis_queries.sql           # Example queries
├── tests/
│   ├── conftest.py
│   ├── test_*.py                      # Unit tests
└── pyproject.toml                     # Project config
```

### Makefile Targets

```bash
make help              # Show available commands
make install           # Install dependencies
make test              # Run tests
make lint              # Run linters
make format            # Format code
make airflow-up        # Start Airflow
make airflow-down      # Stop Airflow
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- [SpaceNews API](https://api.spaceflightnewsapi.net/v4/docs) for providing space industry data
- Apache Airflow, AWS Glue, and PySpark communities

---

<div align="center">
Made with ❤️ by the Data Engineering Team
</div>
