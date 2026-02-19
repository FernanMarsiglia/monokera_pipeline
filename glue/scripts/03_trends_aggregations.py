"""
Glue Job 03: Trends & Aggregations
Gold content_enriched → Gold trends

Input: data/gold/content_enriched/{endpoint}/ingest_date=YYYY-MM-DD/*.parquet
Output: 
  - data/gold/trends/topic_counts/ingest_date=YYYY-MM-DD/*.parquet
  - data/gold/trends/source_activity/ingest_date=YYYY-MM-DD/*.parquet
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Import spark_utils
from spark_utils import (
    get_spark, parse_args, log_dataframe_stats, write_with_stats
)


def calculate_topic_trends(df: DataFrame, ingest_date: str) -> DataFrame:
    """
    Calculate monthly topic trends
    
    Returns:
        DataFrame with columns: year_month, topic_name, content_type, content_count, ingest_date
    """
    print("\n📊 Calculating topic trends...")
    
    # Create year_month field
    df = df.withColumn(
        "year_month",
        F.concat(
            F.col("published_year").cast("string"),
            F.lit("-"),
            F.format_string("%02d", F.col("published_month"))
        )
    )
    
    # Group by year_month, topic_name, content_type
    trends = df.groupBy("year_month", "topic_name", "content_type") \
        .agg(
            F.count("*").alias("content_count")
        ) \
        .withColumn("ingest_date", F.lit(ingest_date).cast(DateType())) \
        .withColumn("calculated_at", F.current_timestamp())
    
    # Sort by year_month desc, content_count desc
    trends = trends.orderBy(
        F.desc("year_month"),
        F.desc("content_count")
    )
    
    trend_count = trends.count()
    print(f"  Generated {trend_count:,} trend records")
    
    # Show top trends
    print("\n  Top 5 trends (latest month):")
    latest_month = trends.select(F.max("year_month")).collect()[0][0]
    top_trends = trends.filter(F.col("year_month") == latest_month).limit(5)
    
    for row in top_trends.collect():
        print(f"    {row['year_month']} | {row['topic_name']:15} | "
              f"{row['content_type']:10} | {row['content_count']:5,}")
    
    return trends


def calculate_source_activity(df: DataFrame, ingest_date: str) -> DataFrame:
    """
    Calculate monthly source activity (news_site)
    
    Returns:
        DataFrame with columns: year_month, news_site, content_type, content_count, ingest_date
    """
    print("\n📊 Calculating source activity...")
    
    # Create year_month field
    df = df.withColumn(
        "year_month",
        F.concat(
            F.col("published_year").cast("string"),
            F.lit("-"),
            F.format_string("%02d", F.col("published_month"))
        )
    )
    
    # Group by year_month, news_site, content_type
    activity = df.groupBy("year_month", "news_site", "content_type") \
        .agg(
            F.count("*").alias("content_count")
        ) \
        .withColumn("ingest_date", F.lit(ingest_date).cast(DateType())) \
        .withColumn("calculated_at", F.current_timestamp())
    
    # Sort
    activity = activity.orderBy(
        F.desc("year_month"),
        F.desc("content_count")
    )
    
    activity_count = activity.count()
    print(f"  Generated {activity_count:,} activity records")
    
    # Show top sources
    print("\n  Top 5 sources (latest month):")
    latest_month = activity.select(F.max("year_month")).collect()[0][0]
    top_sources = activity.filter(F.col("year_month") == latest_month).limit(5)
    
    for row in top_sources.collect():
        print(f"    {row['year_month']} | {row['news_site']:25} | "
              f"{row['content_type']:10} | {row['content_count']:5,}")
    
    return activity


def main():
    """Main execution function"""
    print(f"\n{'='*80}")
    print("Glue Job 03: Trends & Aggregations (Gold → Gold Trends)")
    print(f"Started at: {datetime.now().isoformat()}")
    print(f"{'='*80}\n")
    
    # Parse arguments
    args = parse_args([
        "S3_BUCKET",
        "INGEST_DATE"
    ])
    
    s3_bucket = args["S3_BUCKET"]
    ingest_date = args["INGEST_DATE"]
    gold_prefix = args.get("GOLD_PREFIX", "data/gold")
    
    print(f"Parameters:")
    print(f"  S3_BUCKET: {s3_bucket}")
    print(f"  INGEST_DATE: {ingest_date}")
    print(f"  GOLD_PREFIX: {gold_prefix}")
    print()
    
    # Initialize Spark
    spark = get_spark("SpaceNews-03-Trends")
    
    # Read all enriched content (articles, blogs, reports)
    # Note: We skip 'info' as it doesn't have the same structure
    content_types = ["articles", "blogs", "reports"]
    
    dfs = []
    for content_type in content_types:
        path = f"s3://{s3_bucket}/{gold_prefix}/content_enriched/{content_type}/ingest_date={ingest_date}/"
        
        try:
            print(f"Reading {content_type} from: {path}")
            df = spark.read.parquet(path)
            count = df.count()
            print(f"  Found {count:,} records")
            
            if count > 0:
                dfs.append(df)
        
        except Exception as e:
            print(f"  ⚠️  Could not read {content_type}: {str(e)}")
    
    if not dfs:
        print("\n❌ No data found to process. Exiting.")
        spark.stop()
        return
    
    # Union all content - normalize columns first
    print(f"\n📦 Combining {len(dfs)} content types...")
    
    # Get all unique columns from all DataFrames
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    all_columns = sorted(list(all_columns))
    
    print(f"  Normalizing to {len(all_columns)} columns...")
    
    # Normalize each DataFrame to have all columns
    normalized_dfs = []
    for df in dfs:
        # Add missing columns with NULL values
        for col in all_columns:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(None))
        
        # Select columns in same order
        df = df.select(*all_columns)
        normalized_dfs.append(df)
    
    # Now union all
    all_content = normalized_dfs[0]
    for df in normalized_dfs[1:]:
        all_content = all_content.union(df)
    
    total_records = all_content.count()
    print(f"Total records to analyze: {total_records:,}")
    
    # Cache for reuse
    all_content.cache()
    
    # Calculate topic trends
    topic_trends = calculate_topic_trends(all_content, ingest_date)
    
    # Write topic trends (Parquet for analytics)
    topic_trends_path = f"s3://{s3_bucket}/{gold_prefix}/trends/topic_counts/ingest_date={ingest_date}/"
    print(f"\n💾 Writing topic trends to: {topic_trends_path}")
    write_with_stats(
        topic_trends,
        topic_trends_path,
        mode="overwrite",
        format="parquet"
    )
    
    # Write topic trends (CSV for Redshift)
    topic_trends_csv_path = f"s3://{s3_bucket}/{gold_prefix}/trends/topic_counts_csv/ingest_date={ingest_date}/"
    print(f"\n💾 Writing topic trends CSV to: {topic_trends_csv_path}")
    topic_trends.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("escape", '"') \
        .option("quote", '"') \
        .csv(topic_trends_csv_path)
    
    # Calculate source activity
    source_activity = calculate_source_activity(all_content, ingest_date)
    
    # Write source activity (Parquet for analytics)
    source_activity_path = f"s3://{s3_bucket}/{gold_prefix}/trends/source_activity/ingest_date={ingest_date}/"
    print(f"\n💾 Writing source activity to: {source_activity_path}")
    write_with_stats(
        source_activity,
        source_activity_path,
        mode="overwrite",
        format="parquet"
    )
    
    # Write source activity (CSV for Redshift)
    source_activity_csv_path = f"s3://{s3_bucket}/{gold_prefix}/trends/source_activity_csv/ingest_date={ingest_date}/"
    print(f"\n💾 Writing source activity CSV to: {source_activity_csv_path}")
    source_activity.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("escape", '"') \
        .option("quote", '"') \
        .csv(source_activity_csv_path)
    
    # Unpersist cache
    all_content.unpersist()
    
    # Summary
    print(f"\n{'='*80}")
    print("JOB SUMMARY")
    print(f"{'='*80}")
    print(f"Input records analyzed: {total_records:,}")
    print(f"Topic trend records: {topic_trends.count():,}")
    print(f"Source activity records: {source_activity.count():,}")
    print(f"{'='*80}")
    
    print(f"\n✅ Job completed at: {datetime.now().isoformat()}")
    
    spark.stop()


if __name__ == "__main__":
    main()
