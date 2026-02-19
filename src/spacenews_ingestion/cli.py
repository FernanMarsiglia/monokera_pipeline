"""
CLI Entrypoints for SpaceNews Ingestion
Provides command-line interface for extraction tasks
"""

import click
import sys
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from spacenews_ingestion.api_client import SpaceNewsApiClient
from spacenews_ingestion.paginator import Paginator
from spacenews_ingestion.validation import Validator
from spacenews_ingestion.logging_config import get_logger, LogContext
from spacenews_ingestion.alerts import SNSAlerter

try:
    import boto3
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

logger = get_logger(__name__)


def write_to_s3(
    bucket: str,
    key: str,
    data: str,
    aws_access_key: Optional[str] = None,
    aws_secret_key: Optional[str] = None
):
    """Write data to S3"""
    if not BOTO3_AVAILABLE:
        raise RuntimeError("boto3 is required for S3 operations")
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data.encode('utf-8')
    )
    
    logger.info(f"Wrote to s3://{bucket}/{key}", extra={"s3_path": f"s3://{bucket}/{key}"})


@click.group()
def cli():
    """SpaceNews Ingestion CLI"""
    pass


@cli.command()
@click.option('--endpoint', required=True, type=click.Choice(['articles', 'blogs', 'reports', 'info']),
              help='API endpoint to extract')
@click.option('--ingest-date', default=None, help='Ingest date (YYYY-MM-DD), defaults to today')
@click.option('--since', default=None, help='Fetch records published since date (YYYY-MM-DD)')
@click.option('--updated-after', default=None, help='Fetch records updated after date (YYYY-MM-DD)')
@click.option('--limit', default=None, type=int, help='Maximum number of records to fetch')
@click.option('--page-size', default=100, type=int, help='Number of records per page')
@click.option('--bucket', default='monokera-bucket', help='S3 bucket name')
@click.option('--prefix', default='data/bronze', help='S3 prefix for output')
@click.option('--dry-run', is_flag=True, help='Print records instead of writing to S3')
@click.option('--run-id', default=None, help='Unique run identifier')
@click.option('--sns-topic', default=None, help='SNS topic ARN for alerts')
@click.option('--validate/--no-validate', default=True, help='Validate records before writing')
def extract(
    endpoint: str,
    ingest_date: Optional[str],
    since: Optional[str],
    updated_after: Optional[str],
    limit: Optional[int],
    page_size: int,
    bucket: str,
    prefix: str,
    dry_run: bool,
    run_id: Optional[str],
    sns_topic: Optional[str],
    validate: bool
):
    """
    Extract data from SpaceNews API endpoint
    
    Example:
        python -m spacenews_ingestion.cli extract --endpoint articles --ingest-date 2026-02-16
    """
    # Set defaults
    ingest_date = ingest_date or datetime.now().strftime("%Y-%m-%d")
    run_id = run_id or f"cli_{endpoint}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger = get_logger(__name__, correlation_id=run_id)
    
    logger.info(
        "Starting extraction",
        extra={
            "run_id": run_id,
            "endpoint": endpoint,
            "ingest_date": ingest_date,
            "since": since,
            "updated_after": updated_after,
            "limit": limit,
            "page_size": page_size,
            "dry_run": dry_run
        }
    )
    
    # Initialize components
    client = SpaceNewsApiClient(base_url="https://api.spaceflightnewsapi.net/v4")
    validator = Validator(endpoint) if validate else None
    alerter = SNSAlerter(topic_arn=sns_topic, dry_run=(dry_run or not sns_topic))
    
    start_time = datetime.now()
    
    try:
        # Build query parameters
        params = {"limit": page_size}
        if since:
            params["published_at_gte"] = since
        if updated_after:
            params["updated_at_gte"] = updated_after
        
        # Create paginator
        paginator = Paginator(
            fetch_func=client.get,
            endpoint=f"/{endpoint}",
            params=params,
            page_size=page_size,
            max_results=limit
        )
        
        # Collect records
        records = []
        valid_count = 0
        invalid_count = 0
        
        for idx, record in enumerate(paginator, 1):
            # Validate if enabled
            if validator:
                is_valid, error_reason = validator.validate(record)
                if not is_valid:
                    logger.warning(
                        f"Invalid record {idx}",
                        extra={"record_id": record.get("id"), "error_reason": error_reason}
                    )
                    invalid_count += 1
                    continue
            
            records.append(record)
            valid_count += 1
            
            # Log progress every 100 records
            if idx % 100 == 0:
                logger.info(f"Processed {idx} records", extra={"record_count": idx})
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(
            "Extraction complete",
            extra={
                "run_id": run_id,
                "endpoint": endpoint,
                "valid_count": valid_count,
                "invalid_count": invalid_count,
                "duration_seconds": duration
            }
        )
        
        # Write to S3 or print
        if dry_run:
            print(f"\n=== DRY RUN: Would write {len(records)} records ===")
            print(f"Sample record:\n{json.dumps(records[0] if records else {}, indent=2)}")
        else:
            # Write data as JSONL
            jsonl_data = "\n".join(json.dumps(r) for r in records)
            
            # S3 path: data/bronze/{endpoint}/ingest_date=YYYY-MM-DD/part-0000.jsonl
            s3_key = f"{prefix}/{endpoint}/ingest_date={ingest_date}/part-0000.jsonl"
            
            write_to_s3(
                bucket=bucket,
                key=s3_key,
                data=jsonl_data,
                aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            
            # Write metadata
            metadata = {
                "endpoint": endpoint,
                "run_id": run_id,
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat(),
                "records_downloaded": len(records),
                "valid_count": valid_count,
                "invalid_count": invalid_count,
                "pagination": paginator.get_stats(),
                "ingest_date": ingest_date
            }
            
            meta_key = f"{prefix}/_meta/ingest_date={ingest_date}/{endpoint}_metadata.json"
            write_to_s3(
                bucket=bucket,
                key=meta_key,
                data=json.dumps(metadata, indent=2),
                aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            
            click.echo(f"✅ Extracted {valid_count} valid records to s3://{bucket}/{s3_key}")
            click.echo(f"📊 Metadata written to s3://{bucket}/{meta_key}")
        
        # Send success alert
        alerter.alert_pipeline_success(
            run_id=run_id,
            records_processed=valid_count,
            duration_seconds=duration,
            ingest_date=ingest_date
        )
    
    except Exception as e:
        logger.exception("Extraction failed", extra={"run_id": run_id, "endpoint": endpoint})
        
        # Send failure alert
        alerter.alert_extraction_failure(
            endpoint=endpoint,
            error=e,
            run_id=run_id,
            records_fetched=len(records) if 'records' in locals() else 0
        )
        
        click.echo(f"❌ Extraction failed: {str(e)}", err=True)
        sys.exit(1)
    
    finally:
        client.close()


@cli.command()
@click.option('--bucket', default='monokera-bucket', help='S3 bucket name')
@click.option('--prefix', default='data/bronze', help='S3 prefix to list')
def list_extractions(bucket: str, prefix: str):
    """List recent extraction runs"""
    if not BOTO3_AVAILABLE:
        click.echo("❌ boto3 is required for S3 operations", err=True)
        sys.exit(1)
    
    s3 = boto3.client('s3')
    
    try:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{prefix}/_meta/",
            MaxKeys=20
        )
        
        if 'Contents' not in response:
            click.echo("No metadata files found")
            return
        
        click.echo(f"\n📂 Recent extractions in s3://{bucket}/{prefix}/:")
        click.echo("=" * 80)
        
        for obj in sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True):
            key = obj['Key']
            size = obj['Size']
            modified = obj['LastModified'].strftime("%Y-%m-%d %H:%M:%S")
            
            # Try to read metadata
            try:
                meta_obj = s3.get_object(Bucket=bucket, Key=key)
                meta = json.loads(meta_obj['Body'].read())
                endpoint = meta.get('endpoint', 'unknown')
                records = meta.get('records_downloaded', 0)
                
                click.echo(f"{modified} | {endpoint:10} | {records:5} records | {key}")
            except:
                click.echo(f"{modified} | {key}")
        
        click.echo("=" * 80)
    
    except Exception as e:
        click.echo(f"❌ Failed to list extractions: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--ingest-date', required=True, help='Ingest date (YYYY-MM-DD)')
@click.option('--bucket', default='monokera-bucket', help='S3 bucket name')
def validate_extraction(ingest_date: str, bucket: str):
    """Validate extraction data for specific ingest date"""
    if not BOTO3_AVAILABLE:
        click.echo("❌ boto3 is required for S3 operations", err=True)
        sys.exit(1)
    
    s3 = boto3.client('s3')
    endpoints = ['articles', 'blogs', 'reports', 'info']
    
    click.echo(f"\n🔍 Validating extraction for {ingest_date}:")
    click.echo("=" * 80)
    
    total_records = 0
    
    for endpoint in endpoints:
        prefix = f"data/bronze/{endpoint}/ingest_date={ingest_date}/"
        
        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if 'Contents' not in response:
                click.echo(f"❌ {endpoint:10} | No files found")
                continue
            
            # Count total size
            total_size = sum(obj['Size'] for obj in response['Contents'])
            file_count = len(response['Contents'])
            
            # Read first file to count records
            first_file = response['Contents'][0]['Key']
            obj = s3.get_object(Bucket=bucket, Key=first_file)
            content = obj['Body'].read().decode('utf-8')
            records = len([line for line in content.split('\n') if line.strip()])
            
            total_records += records
            
            click.echo(f"✅ {endpoint:10} | {records:5} records | {file_count} files | {total_size/1024:.1f} KB")
        
        except Exception as e:
            click.echo(f"❌ {endpoint:10} | Error: {str(e)}")
    
    click.echo("=" * 80)
    click.echo(f"Total records: {total_records}")


if __name__ == "__main__":
    cli()
