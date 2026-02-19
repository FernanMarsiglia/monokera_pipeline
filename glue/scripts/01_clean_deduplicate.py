"""
Glue Job 01: Clean & Deduplicate
Bronze → Silver transformation with data quality checks

Input: data/bronze/{endpoint}/ingest_date=YYYY-MM-DD/*.jsonl
Output: data/silver/{endpoint}/ingest_date=YYYY-MM-DD/*.parquet
Quarantine: data/silver/quarantine/{endpoint}/ingest_date=YYYY-MM-DD/*.parquet
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Import spark_utils (must be in same S3 folder or uploaded separately)
from spark_utils import (
    get_spark, parse_args, safe_parse_timestamp,
    calculate_record_hash, deduplicate_by_key,
    log_dataframe_stats, write_with_stats
)


def validate_record(df: DataFrame) -> tuple:
    """
    Validate records and separate valid from invalid
    
    Returns:
        Tuple of (valid_df, invalid_df)
    """
    # Add validation flags
    df = df.withColumn("_is_valid", F.lit(True))
    df = df.withColumn("_error_reason", F.lit(None).cast(StringType()))
    
    # Check required fields
    required_fields = ["id", "title", "url", "published_at"]
    
    for field in required_fields:
        df = df.withColumn(
            "_is_valid",
            F.when(
                F.col(field).isNull() | (F.col(field) == ""),
                False
            ).otherwise(F.col("_is_valid"))
        )
        df = df.withColumn(
            "_error_reason",
            F.when(
                (F.col(field).isNull() | (F.col(field) == "")) & F.col("_error_reason").isNull(),
                F.lit(f"Missing required field: {field}")
            ).otherwise(F.col("_error_reason"))
        )
    
    # Check timestamp validity (if published_at_parsed is null after parsing)
    df = df.withColumn(
        "_is_valid",
        F.when(
            F.col("published_at").isNotNull() & F.col("published_at_parsed").isNull(),
            False
        ).otherwise(F.col("_is_valid"))
    )
    df = df.withColumn(
        "_error_reason",
        F.when(
            F.col("published_at").isNotNull() & F.col("published_at_parsed").isNull() & F.col("_error_reason").isNull(),
            F.lit("Invalid timestamp format for published_at")
        ).otherwise(F.col("_error_reason"))
    )
    
    # Separate valid and invalid
    valid_df = df.filter(F.col("_is_valid") == True).drop("_is_valid", "_error_reason")
    invalid_df = df.filter(F.col("_is_valid") == False)
    
    return valid_df, invalid_df


def process_info(
    spark: SparkSession,
    s3_bucket: str,
    bronze_prefix: str,
    silver_prefix: str,
    ingest_date: str
) -> list:
    """
    Process API info metadata and extract valid news_sites list
    
    Returns:
        List of valid news_sites from API, empty list if not found
    """
    print(f"\n{'='*80}")
    print(f"Processing API Info (metadata)")
    print(f"{'='*80}")
    
    bronze_path = f"s3://{s3_bucket}/{bronze_prefix}/info/ingest_date={ingest_date}/"
    
    try:
        df = spark.read.json(bronze_path)
        
        if df.count() == 0:
            print("⚠️  No info data found")
            return []
        
        # Extract API version and news_sites
        row = df.first()
        api_version = row.version if 'version' in row.asDict() else None
        news_sites = row.news_sites if 'news_sites' in row.asDict() else []
        
        print(f"  API Version: {api_version}")
        print(f"  Valid News Sites: {len(news_sites)}")
        print(f"  Sample sites: {', '.join(news_sites[:5])}...")
        
        # Create structured DataFrame for Silver
        info_df = spark.createDataFrame([
            {
                "api_version": api_version,
                "news_sites_count": len(news_sites),
                "news_sites_list": news_sites,
                "ingest_date": ingest_date,
                "captured_at": datetime.now().isoformat()
            }
        ])
        
        # Write to Silver as metadata
        silver_path = f"s3://{s3_bucket}/{silver_prefix}/api_metadata/ingest_date={ingest_date}/"
        print(f"💾 Writing API metadata to: {silver_path}")
        info_df.write.mode("overwrite").parquet(silver_path)
        
        return news_sites
        
    except Exception as e:
        print(f"⚠️  Error processing info: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


def process_content_type(
    spark: SparkSession,
    content_type: str,
    s3_bucket: str,
    bronze_prefix: str,
    silver_prefix: str,
    quarantine_prefix: str,
    ingest_date: str,
    valid_news_sites: list = None
) -> dict:
    """
    Process one content type (articles, blogs, reports)
    Validates news_site against official list from API info
    
    Returns:
        Dictionary with processing statistics
    """
    print(f"\n{'='*80}")
    print(f"Processing content_type: {content_type}")
    print(f"{'='*80}")
    
    # Input path
    bronze_path = f"s3://{s3_bucket}/{bronze_prefix}/{content_type}/ingest_date={ingest_date}/"
    
    print(f"Reading from: {bronze_path}")
    
    try:
        # Read JSONL from bronze
        df = spark.read.json(bronze_path)
        
        initial_count = log_dataframe_stats(df, f"{content_type}_bronze_read")
        
        if initial_count == 0:
            print(f"⚠️  No records found for {content_type}")
            return {
                "content_type": content_type,
                "initial_count": 0,
                "valid_count": 0,
                "invalid_count": 0,
                "deduplicated_count": 0
            }
        
        # Add metadata columns
        df = df.withColumn("content_type", F.lit(content_type))
        df = df.withColumn("source_system", F.lit("spaceflight_news_api_v4"))
        df = df.withColumn("ingest_date", F.lit(ingest_date).cast(DateType()))
        df = df.withColumn("content_id", F.concat(F.lit(f"{content_type}_"), F.col("id").cast("string")))
        
        # Validate news_site against official list
        if valid_news_sites and len(valid_news_sites) > 0:
            df = df.withColumn(
                "news_site_validated",
                F.when(F.col("news_site").isin(valid_news_sites), True).otherwise(False)
            )
            
            invalid_sites_count = df.filter(F.col("news_site_validated") == False).count()
            if invalid_sites_count > 0:
                print(f"  ⚠️  Found {invalid_sites_count} records with non-official news_sites")
        else:
            df = df.withColumn("news_site_validated", F.lit(None).cast("boolean"))
        
        # Safe parse timestamps
        df = safe_parse_timestamp(df, "published_at", "published_at_parsed")
        if "updated_at" in df.columns:
            df = safe_parse_timestamp(df, "updated_at", "updated_at_parsed")
        else:
            df = df.withColumn("updated_at", F.lit(None).cast(StringType()))
            df = df.withColumn("updated_at_parsed", F.lit(None).cast(TimestampType()))
        
        # Validate records
        valid_df, invalid_df = validate_record(df)
        
        valid_count = valid_df.count() if valid_df else 0
        invalid_count = invalid_df.count() if invalid_df else 0
        
        print(f"✅ Valid records: {valid_count:,}")
        print(f"❌ Invalid records: {invalid_count:,}")
        
        # Write invalid records to quarantine
        if invalid_count > 0:
            quarantine_path = f"s3://{s3_bucket}/{silver_prefix}/quarantine/{content_type}/ingest_date={ingest_date}/"
            
            # Add quarantine metadata
            invalid_df = invalid_df.withColumn("quarantine_timestamp", F.current_timestamp())
            invalid_df = invalid_df.withColumn("raw_payload", F.to_json(F.struct("*")))
            
            print(f"📦 Writing {invalid_count} invalid records to quarantine: {quarantine_path}")
            invalid_df.write.mode("overwrite").parquet(quarantine_path)
        
        if valid_count == 0:
            print(f"⚠️  No valid records to process for {content_type}")
            return {
                "content_type": content_type,
                "initial_count": initial_count,
                "valid_count": 0,
                "invalid_count": invalid_count,
                "deduplicated_count": 0
            }
        
        # Replace original timestamp columns with parsed versions
        valid_df = valid_df.withColumnRenamed("published_at", "published_at_original")
        valid_df = valid_df.withColumnRenamed("published_at_parsed", "published_at")
        
        if "updated_at_parsed" in valid_df.columns:
            valid_df = valid_df.withColumnRenamed("updated_at", "updated_at_original")
            valid_df = valid_df.withColumnRenamed("updated_at_parsed", "updated_at")
        
        # Calculate record hash for deduplication
        hash_columns = ["id", "title", "url", "published_at", "summary"]
        hash_columns = [c for c in hash_columns if c in valid_df.columns]
        valid_df = calculate_record_hash(valid_df, hash_columns, "record_hash")
        
        # Deduplicate by (content_type, content_id), keeping latest updated_at
        order_column = "updated_at" if "updated_at" in valid_df.columns else "published_at"
        deduped_df = deduplicate_by_key(
            valid_df,
            key_columns=["content_type", "content_id"],
            order_column=order_column,
            ascending=False  # Keep latest
        )
        
        deduplicated_count = deduped_df.count()
        duplicates_removed = valid_count - deduplicated_count
        
        print(f"🔄 Deduplicated: {deduplicated_count:,} records (removed {duplicates_removed} duplicates)")
        
        # Select final schema for Silver
        # Keep all original fields + metadata
        final_columns = [
            "id",
            "title",
            "url",
            "image_url",
            "news_site",
            "summary",
            "published_at",
            "updated_at",
            "featured",
            "launches",
            "events",
            "authors",
            "content_id",
            "content_type",
            "source_system",
            "ingest_date",
            "news_site_validated",
            "record_hash"
        ]
        
        # Only select columns that exist
        final_columns = [c for c in final_columns if c in deduped_df.columns]
        deduped_df = deduped_df.select(*final_columns)
        
        # Write to Silver
        silver_path = f"s3://{s3_bucket}/{silver_prefix}/{content_type}/ingest_date={ingest_date}/"
        
        print(f"💾 Writing to Silver: {silver_path}")
        write_with_stats(
            deduped_df,
            silver_path,
            mode="overwrite",
            format="parquet",
            partition_by=None  # Already partitioned by folder structure
        )
        
        return {
            "content_type": content_type,
            "initial_count": initial_count,
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "deduplicated_count": deduplicated_count,
            "duplicates_removed": duplicates_removed
        }
    
    except Exception as e:
        print(f"❌ Error processing {content_type}: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "content_type": content_type,
            "initial_count": 0,
            "valid_count": 0,
            "invalid_count": 0,
            "deduplicated_count": 0,
            "error": str(e)
        }


def main():
    """Main execution function"""
    print(f"\n{'='*80}")
    print("Glue Job 01: Clean & Deduplicate (Bronze → Silver)")
    print(f"Started at: {datetime.now().isoformat()}")
    print(f"{'='*80}\n")
    
    # Parse arguments
    args = parse_args([
        "S3_BUCKET",
        "INGEST_DATE"
    ])
    
    s3_bucket = args["S3_BUCKET"]
    ingest_date = args["INGEST_DATE"]
    bronze_prefix = args.get("BRONZE_PREFIX", "data/bronze")
    silver_prefix = args.get("SILVER_PREFIX", "data/silver")
    quarantine_prefix = args.get("QUARANTINE_PREFIX", "data/silver/quarantine")
    
    print(f"Parameters:")
    print(f"  S3_BUCKET: {s3_bucket}")
    print(f"  INGEST_DATE: {ingest_date}")
    print(f"  BRONZE_PREFIX: {bronze_prefix}")
    print(f"  SILVER_PREFIX: {silver_prefix}")
    print(f"  QUARANTINE_PREFIX: {quarantine_prefix}")
    print()
    
    # Initialize Spark
    spark = get_spark("SpaceNews-01-CleanDedup")
    
    # Step 1: Process API info first to extract valid news_sites
    print("\n🔍 Step 1: Loading API metadata for validation...")
    valid_news_sites = process_info(
        spark,
        s3_bucket,
        bronze_prefix,
        silver_prefix,
        ingest_date
    )
    
    if valid_news_sites:
        print(f"✅ Loaded {len(valid_news_sites)} official news sites for validation\n")
    else:
        print("⚠️  No official news sites loaded - skipping validation\n")
    
    # Step 2: Process content types with validation
    print("🔄 Step 2: Processing content types...")
    content_types = ["articles", "blogs", "reports"]
    results = []
    
    for content_type in content_types:
        result = process_content_type(
            spark,
            content_type,
            s3_bucket,
            bronze_prefix,
            silver_prefix,
            quarantine_prefix,
            ingest_date,
            valid_news_sites=valid_news_sites
        )
        results.append(result)
    
    # Summary
    print(f"\n{'='*80}")
    print("JOB SUMMARY")
    print(f"{'='*80}")
    
    total_initial = sum(r["initial_count"] for r in results)
    total_valid = sum(r["valid_count"] for r in results)
    total_invalid = sum(r["invalid_count"] for r in results)
    total_deduplicated = sum(r["deduplicated_count"] for r in results)
    
    for result in results:
        print(f"\n{result['content_type']:10} | "
              f"Initial: {result['initial_count']:5,} | "
              f"Valid: {result['valid_count']:5,} | "
              f"Invalid: {result['invalid_count']:3,} | "
              f"Final: {result['deduplicated_count']:5,}")
        
        if "error" in result:
            print(f"           ❌ Error: {result['error']}")
    
    print(f"\n{'='*80}")
    print(f"TOTAL      | "
          f"Initial: {total_initial:5,} | "
          f"Valid: {total_valid:5,} | "
          f"Invalid: {total_invalid:3,} | "
          f"Final: {total_deduplicated:5,}")
    print(f"{'='*80}")
    
    print(f"\n✅ Job completed at: {datetime.now().isoformat()}")
    
    spark.stop()


if __name__ == "__main__":
    main()
