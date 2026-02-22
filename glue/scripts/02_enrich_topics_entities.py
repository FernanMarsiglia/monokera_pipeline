"""
Glue Job 02: Enrich Topics & Entities
Silver → Gold transformation with business enrichments

Input: data/silver/{endpoint}/ingest_date=YYYY-MM-DD/*.parquet
Output: data/gold/content_enriched/{endpoint}/ingest_date=YYYY-MM-DD/*.parquet
"""

import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Import spark_utils
from spark_utils import (
    get_spark, parse_args, log_dataframe_stats, write_with_stats,
    register_udfs, classify_topic, extract_entities, tokenize_text,
    TOPIC_KEYWORDS, ENTITY_DICT
)


def enrich_content(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Apply business enrichments to content
    
    Adds:
    - keywords (comma-separated string from title + summary)
    - entities (comma-separated string)
    - topic_name (classification)
    - published_year, published_month, published_day
    - authors_json, launches_json, events_json (converted to JSON strings)
    - ingest_run_id, processed_at
    """
    # Register UDFs
    tokenize_udf, classify_topic_udf, extract_entities_udf = register_udfs(spark)
    
    # Combine title + summary for analysis
    df = df.withColumn(
        "_text_for_analysis",
        F.concat_ws(" ", F.coalesce(F.col("title"), F.lit("")), F.coalesce(F.col("summary"), F.lit("")))
    )
    
    # Extract keywords (tokenization)
    df = df.withColumn("_keywords_array", tokenize_udf(F.col("_text_for_analysis")))
    df = df.withColumn("keywords", F.concat_ws(",", F.col("_keywords_array")))
    
    # Extract entities
    df = df.withColumn("_entities_array", extract_entities_udf(F.col("_text_for_analysis")))
    df = df.withColumn("entities", F.concat_ws(",", F.col("_entities_array")))
    
    # Classify topic
    df = df.withColumn("topic_name", classify_topic_udf(F.col("_text_for_analysis")))

    # Extract temporal fields from published_at
    df = df.withColumn("published_year", F.year(F.col("published_at")))
    df = df.withColumn("published_month", F.month(F.col("published_at")))
    df = df.withColumn("published_day", F.dayofmonth(F.col("published_at")))

    # DEBUG: Show sample topics to verify no years appear
    print("\n🔍 DEBUG - Sample topic classifications (first 20):")
    sample_topics = df.select("id", "title", "topic_name", "published_year").limit(20)
    for row in sample_topics.collect():
        print(f"  ID={row['id']:6} | topic={row['topic_name']:15} | year={row['published_year']} | title={row['title'][:50]}")
    
    # Convert array/struct fields to JSON strings
    # authors
    if "authors" in df.columns:
        df = df.withColumn("authors_json", 
            F.when(F.col("authors").isNotNull(), F.to_json(F.col("authors")))
            .otherwise(F.lit("[]"))
        )
    else:
        df = df.withColumn("authors_json", F.lit("[]"))
    
    # launches
    if "launches" in df.columns:
        df = df.withColumn("launches_json",
            F.when(F.col("launches").isNotNull(), F.to_json(F.col("launches")))
            .otherwise(F.lit("[]"))
        )
    else:
        df = df.withColumn("launches_json", F.lit("[]"))
    
    # events
    if "events" in df.columns:
        df = df.withColumn("events_json",
            F.when(F.col("events").isNotNull(), F.to_json(F.col("events")))
            .otherwise(F.lit("[]"))
        )
    else:
        df = df.withColumn("events_json", F.lit("[]"))
    
    # Add processing metadata
    run_id = f"glue_02_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    df = df.withColumn("ingest_run_id", F.lit(run_id))
    df = df.withColumn("processed_at", F.current_timestamp())
    
    # Drop intermediate columns
    df = df.drop("_text_for_analysis", "_keywords_array", "_entities_array")
    
    # Drop original array columns (we have JSON versions now)
    columns_to_drop = ["authors", "launches", "events"]
    for col_name in columns_to_drop:
        if col_name in df.columns:
            df = df.drop(col_name)
    
    return df


def process_content_type(
    spark: SparkSession,
    content_type: str,
    s3_bucket: str,
    silver_prefix: str,
    gold_prefix: str,
    ingest_date: str
) -> dict:
    """
    Process one content type enrichment
    
    Returns:
        Dictionary with processing statistics
    """
    print(f"\n{'='*80}")
    print(f"Enriching content_type: {content_type}")
    print(f"{'='*80}")
    
    # Input path
    silver_path = f"s3://{s3_bucket}/{silver_prefix}/{content_type}/ingest_date={ingest_date}/"
    
    print(f"Reading from Silver: {silver_path}")
    
    try:
        # Check if path exists first
        try:
            df = spark.read.parquet(silver_path)
            print(f"✓ Path exists and readable")
        except Exception as path_err:
            print(f"❌ Cannot read from {silver_path}: {str(path_err)}")
            return {
                "content_type": content_type,
                "initial_count": 0,
                "enriched_count": 0,
                "error": f"Path not found or unreadable: {silver_path}"
            }
        
        initial_count = log_dataframe_stats(df, f"{content_type}_silver_read")
        
        if initial_count == 0:
            print(f"⚠️  No records found in Silver for {content_type}")
            return {
                "content_type": content_type,
                "initial_count": 0,
                "enriched_count": 0
            }
        
        # Cache for reuse
        df.cache()
        
        # Apply enrichments
        print(f"🔧 Applying enrichments...")
        enriched_df = enrich_content(df, spark)
        
        enriched_count = enriched_df.count()
        
        # Sample statistics
        print(f"\n📊 Enrichment Statistics:")
        print(f"  Total records: {enriched_count:,}")
        
        # Show topic distribution
        topic_dist = enriched_df.groupBy("topic_name").count().orderBy(F.desc("count")).limit(5)
        print(f"\n  Top 5 topics:")
        for row in topic_dist.collect():
            print(f"    - {row['topic_name']:15} : {row['count']:5,}")
        
        # DEBUG: Check if any years appear as topics
        print(f"\n🔍 DEBUG - Checking for year values in topic_name...")
        year_topics = enriched_df.filter(
            (F.col("topic_name").rlike("^[0-9]{4}$"))
        ).select("id", "topic_name", "published_year", "title").limit(10)
        
        year_count = year_topics.count()
        if year_count > 0:
            print(f"  ⚠️  WARNING: Found {year_count} records with year as topic_name!")
            for row in year_topics.collect():
                print(f"    ID={row['id']} | topic={row['topic_name']} | year={row['published_year']} | title={row['title'][:60]}")
        else:
            print(f"  ✅ No year values found in topic_name (expected)")
        
        # Show entity statistics
        entity_count = enriched_df.filter(F.col("entities") != "").count()
        print(f"\n  Records with entities: {entity_count:,} ({entity_count/enriched_count*100:.1f}%)")
        
        # Define final schema for Gold
        final_columns = [
            # Base fields
            "id",
            "content_id",
            "content_type",
            "source_system",
            "title",
            "url",
            "summary",
            "image_url",
            "news_site",
            "featured",
            "published_at",
            "updated_at",
            
            # Enrichments
            "keywords",
            "entities",
            "topic_name",
            
            # Temporal
            "published_year",
            "published_month",
            "published_day",
            
            # JSON fields
            "authors_json",
            "launches_json",
            "events_json",
            
            # Metadata
            "ingest_date",
            "ingest_run_id",
            "record_hash",
            "processed_at"
        ]
        
        # Select only columns that exist
        final_columns = [c for c in final_columns if c in enriched_df.columns]
        enriched_df = enriched_df.select(*final_columns)
        
        # Write to Gold (Parquet for analytics)
        gold_path = f"s3://{s3_bucket}/{gold_prefix}/content_enriched/{content_type}/ingest_date={ingest_date}/"
        
        print(f"\n💾 Writing to Gold (Parquet): {gold_path}")
        print(f"   Enriched records count: {enriched_count:,}")
        try:
            write_with_stats(
                enriched_df,
                gold_path,
                mode="overwrite",
                format="parquet",
                partition_by=None  # Already partitioned by folder structure
            )
            print(f"✅ Successfully wrote {enriched_count:,} Parquet records to: {gold_path}")
        except Exception as write_err:
            print(f"❌ CRITICAL ERROR writing Parquet to {gold_path}")
            print(f"   Error Type: {type(write_err).__name__}")
            print(f"   Error Message: {str(write_err)}")
            import traceback
            traceback.print_exc()
            raise
        
        # Also write to CSV for Redshift COPY (more compatible)
        csv_path = f"s3://{s3_bucket}/{gold_prefix}/content_enriched_csv/{content_type}/ingest_date={ingest_date}/"
        
        print(f"\n💾 Writing to Gold (CSV for Redshift): {csv_path}")
        try:
            enriched_df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .option("escape", '"') \
                .option("quote", '"') \
                .csv(csv_path)
            print(f"✅ Successfully wrote CSV to: {csv_path}")
        except Exception as csv_err:
            print(f"❌ CRITICAL ERROR writing CSV to {csv_path}")
            print(f"   Error Type: {type(csv_err).__name__}")
            print(f"   Error Message: {str(csv_err)}")
            import traceback
            traceback.print_exc()
            raise
        
        # Unpersist cache
        df.unpersist()
        
        return {
            "content_type": content_type,
            "initial_count": initial_count,
            "enriched_count": enriched_count
        }
    
    except Exception as e:
        print(f"❌ Error enriching {content_type}: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "content_type": content_type,
            "initial_count": 0,
            "enriched_count": 0,
            "error": str(e)
        }


def main():
    """Main execution function"""
    print(f"\n{'='*80}")
    print("Glue Job 02: Enrich Topics & Entities (Silver → Gold)")
    print(f"Started at: {datetime.now().isoformat()}")
    print(f"{'='*80}\n")
    
    # Parse arguments
    args = parse_args([
        "S3_BUCKET",
        "INGEST_DATE"
    ])
    
    s3_bucket = args["S3_BUCKET"]
    ingest_date = args["INGEST_DATE"]
    silver_prefix = args.get("SILVER_PREFIX", "data/silver")
    gold_prefix = args.get("GOLD_PREFIX", "data/gold")
    
    print(f"Parameters:")
    print(f"  S3_BUCKET: {s3_bucket}")
    print(f"  INGEST_DATE: {ingest_date}")
    print(f"  SILVER_PREFIX: {silver_prefix}")
    print(f"  GOLD_PREFIX: {gold_prefix}")
    print()
    
    # Initialize Spark
    spark = get_spark("SpaceNews-02-Enrich")
    
    # Process each content type
    # Note: Only process types that exist in Silver
    content_types = ["articles", "blogs", "reports"]  # Remove "info" - doesn't exist in Silver
    results = []
    
    for content_type in content_types:
        result = process_content_type(
            spark,
            content_type,
            s3_bucket,
            silver_prefix,
            gold_prefix,
            ingest_date
        )
        results.append(result)
    
    # Summary
    print(f"\n{'='*80}")
    print("JOB SUMMARY")
    print(f"{'='*80}")
    
    total_initial = sum(r["initial_count"] for r in results)
    total_enriched = sum(r["enriched_count"] for r in results)
    errors = [r for r in results if "error" in r]
    
    for result in results:
        print(f"\n{result['content_type']:10} | "
              f"Input: {result['initial_count']:5,} | "
              f"Output: {result['enriched_count']:5,}")
        
        if "error" in result:
            print(f"           ❌ Error: {result['error']}")
    
    print(f"\n{'='*80}")
    print(f"TOTAL      | "
          f"Input: {total_initial:5,} | "
          f"Output: {total_enriched:5,}")
    print(f"{'='*80}")
    
    # If any errors occurred, raise exception to mark job as failed
    if errors:
        error_msgs = "\n".join([f"  • {e['content_type']}: {e.get('error', 'Unknown error')}" for e in errors])
        raise Exception(f"Job failed for {len(errors)} content types:\n{error_msgs}")
    
    if total_enriched == 0 and total_initial == 0:
        raise Exception("Job completed but processed 0 records. Check if Silver data exists for this date.")
    
    print(f"\n✅ Job completed successfully at: {datetime.now().isoformat()}")
    print(f"   Processed: {total_initial:,} → {total_enriched:,} records")
    
    spark.stop()


if __name__ == "__main__":
    main()
