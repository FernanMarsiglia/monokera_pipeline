
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import TimestampType, ArrayType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def get_spark(app_name: str = "GlueJob") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def parse_args(required_args: List[str] = None):
    """Parse Glue job arguments"""
    from awsglue.utils import getResolvedOptions
    import sys
    
    if required_args is None:
        required_args = [
            'S3_BUCKET',
            'RAW_PREFIX',
            'STAGING_PREFIX',
            'CURATED_PREFIX',
            'QUARANTINE_PREFIX'
        ]
    
    args = getResolvedOptions(sys.argv, required_args)
    
    # INGEST_DATE is often optional
    if 'INGEST_DATE' not in required_args:
        try:
            ingest_date_args = getResolvedOptions(sys.argv, ['INGEST_DATE'])
            args['INGEST_DATE'] = ingest_date_args['INGEST_DATE']
        except:
            args['INGEST_DATE'] = None
    
    # Optional prefixes
    optional_params = ['BRONZE_PREFIX', 'SILVER_PREFIX', 'GOLD_PREFIX', 'QUARANTINE_PREFIX']
    for param in optional_params:
        if param not in required_args:
            try:
                opt_args = getResolvedOptions(sys.argv, [param])
                args[param] = opt_args[param]
            except:
                pass
    
    return args

def s3_path_join(*args: str) -> str:
    return "/".join(s.strip("/") for s in args if s)

def safe_timestamp_parse(ts: Any) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
        except Exception:
            return None


def safe_parse_timestamp(df: DataFrame, source_col: str, target_col: str) -> DataFrame:
    """
    Parse timestamp column safely, returning None for invalid timestamps
    
    Args:
        df: Input DataFrame
        source_col: Source column name with string timestamp
        target_col: Target column name for parsed timestamp
    
    Returns:
        DataFrame with new timestamp column
    """
    @F.udf(returnType=TimestampType())
    def parse_ts(ts_str):
        if not ts_str:
            return None
        try:
            # Try ISO format first
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except:
            try:
                # Try with milliseconds
                return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            except:
                try:
                    # Try without milliseconds
                    return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
                except:
                    return None
    
    return df.withColumn(target_col, parse_ts(F.col(source_col)))

def tokenize(text: str, min_len: int = 3) -> List[str]:
    if not text:
        return []
    tokens = re.findall(r"\b\w+\b", text.lower())
    return [t for t in tokens if len(t) >= min_len]

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


# =============================================================================
# NLP / Enrichment Functions
# =============================================================================

# Topic classification keywords
TOPIC_KEYWORDS = {
    "ISS": ["iss", "international space station", "station", "expedition"],
    "Mars": ["mars", "perseverance", "curiosity", "ingenuity", "martian"],
    "Moon": ["moon", "lunar", "artemis", "chandrayaan", "luna"],
    "SpaceX": ["spacex", "starship", "falcon", "dragon", "starlink", "musk"],
    "NASA": ["nasa", "artemis", "sls", "orion"],
    "Astronomy": ["telescope", "james webb", "hubble", "galaxy", "exoplanet", "star"],
    "Satellite": ["satellite", "orbit", "cubesat", "starlink"],
    "Launch": ["launch", "rocket", "liftoff", "countdown"],
    "Science": ["research", "experiment", "discovery", "mission", "data"],
    "Commercial": ["commercial", "private", "company", "industry"],
    "Other": []  # Default fallback
}

# Entity extraction patterns (simplified)
ENTITY_DICT = {
    "spacecraft": ["iss", "hubble", "james webb", "perseverance", "curiosity", "dragon", "starship", "soyuz"],
    "company": ["spacex", "nasa", "esa", "blue origin", "virgin galactic", "roscosmos", "cnsa"],
    "person": ["elon musk", "jeff bezos", "richard branson"],
    "celestial_body": ["mars", "moon", "earth", "jupiter", "saturn", "venus"],
}


def tokenize_text(text: str) -> list:
    """Tokenize and clean text for NLP"""
    if not text:
        return []
    import re
    # Remove special characters, convert to lowercase
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    # Split and filter short words
    tokens = [w for w in text.split() if len(w) >= 3]
    return tokens


def classify_topic(text: str) -> str:
    """
    Classify content topic based on keyword matching
    
    Args:
        text: Content text (title + summary combined)
    
    Returns:
        Topic name (e.g., "Mars", "ISS", "SpaceX")
    """
    if not text:
        return "Other"
    
    text_lower = text.lower()
    tokens = set(tokenize_text(text_lower))
    
    # Score each topic
    topic_scores = {}
    for topic, keywords in TOPIC_KEYWORDS.items():
        if topic == "Other":
            continue
        
        score = 0
        for keyword in keywords:
            if keyword in text_lower:
                score += 2  # Exact match
            
            # Check token overlap
            keyword_tokens = set(keyword.split())
            overlap = len(tokens & keyword_tokens)
            score += overlap
        
        topic_scores[topic] = score
    
    # Return topic with highest score
    if not topic_scores or max(topic_scores.values()) == 0:
        return "Other"
    
    return max(topic_scores, key=topic_scores.get)


def extract_entities(text: str) -> list:
    """
    Extract named entities from text
    
    Args:
        text: Content text
    
    Returns:
        List of entity names found
    """
    if not text:
        return []
    
    text_lower = text.lower()
    entities_found = []
    
    for entity_type, entity_list in ENTITY_DICT.items():
        for entity in entity_list:
            if entity in text_lower:
                entities_found.append(entity)
    
    # Remove duplicates and return
    return list(set(entities_found))


def register_udfs(spark: SparkSession):
    """
    Register custom UDFs with Spark and return UDF objects
    
    Args:
        spark: SparkSession instance
    
    Returns:
        Tuple of (tokenize_udf, classify_topic_udf, extract_entities_udf)
    """
    # Create UDF objects
    tokenize_udf = F.udf(tokenize_text, ArrayType(StringType()))
    classify_topic_udf = F.udf(classify_topic, StringType())
    extract_entities_udf = F.udf(extract_entities, ArrayType(StringType()))
    
    # Also register them for SQL usage
    spark.udf.register("tokenize_text_udf", tokenize_text, ArrayType(StringType()))
    spark.udf.register("classify_topic_udf", classify_topic, StringType())
    spark.udf.register("extract_entities_udf", extract_entities, ArrayType(StringType()))
    
    return tokenize_udf, classify_topic_udf, extract_entities_udf


# =============================================================================
# DataFrame Utility Functions
# =============================================================================


def calculate_record_hash(df: DataFrame, columns: List[str], hash_col_name: str = "record_hash") -> DataFrame:
    """
    Calculate a hash for each record based on specified columns
    
    Args:
        df: Input DataFrame
        columns: List of column names to include in hash
        hash_col_name: Name for the hash column
    
    Returns:
        DataFrame with new hash column
    """
    # Filter only existing columns
    existing_cols = [c for c in columns if c in df.columns]
    
    # Concatenate columns and hash
    df = df.withColumn(
        hash_col_name,
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in existing_cols]), 256)
    )
    
    return df


def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_column: str,
    ascending: bool = False
) -> DataFrame:
    """
    Deduplicate DataFrame by keeping one record per key (based on order column)
    
    Args:
        df: Input DataFrame
        key_columns: Columns that define a unique key
        order_column: Column to use for ordering (e.g., updated_at, published_at)
        ascending: If True, keep earliest; if False, keep latest
    
    Returns:
        Deduplicated DataFrame
    """
    # Create window partitioned by key columns, ordered by order column
    window_spec = Window.partitionBy(*key_columns).orderBy(
        F.col(order_column).asc() if ascending else F.col(order_column).desc()
    )
    
    # Add row number
    df_with_row = df.withColumn("_row_num", F.row_number().over(window_spec))
    
    # Keep only first row per key
    deduped = df_with_row.filter(F.col("_row_num") == 1).drop("_row_num")
    
    return deduped


def log_dataframe_stats(df: DataFrame, stage_name: str = "DataFrame") -> int:
    """
    Log basic statistics about a DataFrame
    
    Args:
        df: DataFrame to analyze
        stage_name: Name/description of this stage
    
    Returns:
        Record count
    """
    count = df.count()
    
    print(f"\n{'='*60}")
    print(f"{stage_name} Statistics")
    print(f"{'='*60}")
    print(f"  Record count: {count:,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Column names: {', '.join(df.columns[:10])}" + (" ..." if len(df.columns) > 10 else ""))
    print(f"{'='*60}\n")
    
    return count


def write_with_stats(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    format: str = "parquet",
    partition_by: Optional[List[str]] = None
) -> None:
    """
    Write DataFrame to S3 with logging
    
    Args:
        df: DataFrame to write
        path: S3 path
        mode: Write mode (overwrite, append, etc.)
        format: Output format (parquet, json, etc.)
        partition_by: Optional list of columns to partition by
    """
    count = df.count()
    
    print(f"\n📝 Writing {count:,} records to: {path}")
    print(f"   Format: {format} | Mode: {mode}")
    
    if partition_by:
        print(f"   Partitions: {', '.join(partition_by)}")
    
    writer = df.write.mode(mode)
    
    if format == "parquet":
        writer = writer.format("parquet")
    elif format == "json":
        writer = writer.format("json")
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.save(path)
    
    print(f"✅ Write completed successfully\n")
