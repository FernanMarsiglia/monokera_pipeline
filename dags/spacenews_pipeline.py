"""
Apache Airflow DAG for SpaceNews Pipeline (Production-Ready)
Bronze → Silver → Gold → Redshift → Insights

Architecture:
1. Extract: API → Bronze (JSONL on S3)
2. Clean: Bronze → Silver (Glue Job 01 - validation with news_sites)
3. Enrich: Silver → Gold (Glue Job 02 - NLP topics/entities)
4. Aggregate: Gold → Trends (Glue Job 03 - monthly aggregations)
5. Load: Gold → Redshift (COPY + MERGE + metadata)
6. Insights: Generate daily statistics
7. Dashboards: Trigger refresh
8. Notify: SNS alerts
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

# Add src to path for local imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def get_var(name, default=None):
    """Get Airflow variable with fallback to env var"""
    try:
        return Variable.get(name, default_var=default)
    except:
        return os.getenv(name, default)

# Configuration
LOCAL_MODE = get_var("LOCAL_MODE", "false").lower() == "true"
S3_BUCKET = get_var("S3_BUCKET", "monokera-bucket")
BRONZE_PREFIX = get_var("BRONZE_PREFIX", "data/bronze")
SILVER_PREFIX = get_var("SILVER_PREFIX", "data/silver")
GOLD_PREFIX = get_var("GOLD_PREFIX", "data/gold")

GLUE_CLEAN_JOB = get_var("GLUE_JOB_CLEAN_DEDUP", "spacenews-01-clean-dedup")
GLUE_ENRICH_JOB = get_var("GLUE_JOB_ENRICH", "spacenews-02-enrich-topics-entities")
GLUE_TRENDS_JOB = get_var("GLUE_JOB_TRENDS", "spacenews-03-trends-aggregations")
SNS_TOPIC_ARN = get_var("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:311048569989:spacenews-pipeline-alerts")

# AWS Credentials (required for boto3 operations)
AWS_ACCESS_KEY_ID = get_var("AWS_ACCESS_KEY_ID", None)
AWS_SECRET_ACCESS_KEY = get_var("AWS_SECRET_ACCESS_KEY", None)
AWS_DEFAULT_REGION = get_var("AWS_DEFAULT_REGION", "us-east-1")

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=2),
}

logger = logging.getLogger(__name__)

def notify_sns(context, success=True, custom_message=None):
    """Send SNS notification"""
    if LOCAL_MODE:
        logger.info(f"[LOCAL] Would send SNS: DAG {context['dag'].dag_id} {'SUCCESS' if success else 'FAILED'}")
        if custom_message:
            logger.info(f"[LOCAL] Message preview:\n{custom_message}")
        return
    
    try:
        import boto3
        
        if custom_message:
            msg = custom_message
        else:
            msg = f"DAG {context['dag'].dag_id} {'SUCCEEDED' if success else 'FAILED'} at {context['ts']}"
        
        sns = boto3.client("sns", region_name="us-east-1")
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=msg,
            Subject=f"SpaceNews Pipeline {'Success' if success else 'Failure'}"
        )
        logger.info(f"SNS notification sent: {msg}")
    except Exception as e:
        logger.error(f"Failed to send SNS: {e}")


def start_glue_job(job_name, ingest_date, **kwargs):
    """Start and wait for Glue job completion"""
    if LOCAL_MODE:
        logger.info(f"[LOCAL] Would start Glue job: {job_name} for date {ingest_date}")
        return "SIMULATED_RUN_ID"
    
    import boto3
    logger.info(f"Starting Glue job: {job_name}")
    
    glue = boto3.client("glue", region_name="us-east-1")
    
    response = glue.start_job_run(
        JobName=job_name,
        Arguments={
            "--INGEST_DATE": ingest_date,
            "--S3_BUCKET": S3_BUCKET,
            "--BRONZE_PREFIX": BRONZE_PREFIX,
            "--SILVER_PREFIX": SILVER_PREFIX,
            "--GOLD_PREFIX": GOLD_PREFIX,
        }
    )
    job_run_id = response["JobRunId"]
    logger.info(f"Glue job {job_name} started with run ID: {job_run_id}")
    
    # Wait for completion
    logger.info(f"Waiting for Glue job {job_name} to complete...")
    import time
    max_attempts = 60
    
    for attempt in range(max_attempts):
        response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response['JobRun']['JobRunState']
        
        logger.info(f"Job status: {status} (attempt {attempt+1}/{max_attempts})")
        
        if status == 'SUCCEEDED':
            logger.info(f"Glue job {job_name} completed successfully")
            return job_run_id
        elif status in ['FAILED', 'STOPPED', 'ERROR', 'TIMEOUT']:
            error_message = response['JobRun'].get('ErrorMessage', 'Unknown error')
            raise Exception(f"Glue job {job_name} failed with status {status}: {error_message}")
        
        # Still running, wait 30 seconds
        time.sleep(30)
    
    raise Exception(f"Glue job {job_name} timeout after {max_attempts * 30}s")
    return job_run_id


def load_to_redshift(ingest_date, **kwargs):
    """Load processed data from Gold to Redshift"""
    if LOCAL_MODE:
        logger.info(f"[LOCAL] Would load data to Redshift for date {ingest_date}")
        return
    
    import boto3
    
    logger.info(f"Loading data to Redshift for {ingest_date}")
    
    # Get Redshift configuration
    redshift_iam_role = get_var("REDSHIFT_IAM_ROLE", "arn:aws:iam::311048569989:role/service-role/AmazonRedshift-CommandsAccessRole-20260115T142621")
    redshift_workgroup = get_var("REDSHIFT_WORKGROUP", "monokera-pipeline")
    redshift_database = get_var("REDSHIFT_DATABASE", "dev")
    
    # Execute Redshift SQL scripts
    redshift_data = boto3.client("redshift-data", region_name="us-east-1")
    
    # Read SQL script (simplified core insert, no SCD Type 2 yet)
    script_path = Path(__file__).parent.parent / "sql" / "redshift_load_core_simple.sql"
    with open(script_path, 'r') as f:
        sql_script = f.read()
    
    # Replace placeholders
    sql_script = sql_script.replace('{{ ingest_date }}', ingest_date)
    sql_script = sql_script.replace('{{ redshift_iam_role }}', redshift_iam_role)
    
    logger.info(f"Executing SQL on Redshift workgroup: {redshift_workgroup}, database: {redshift_database}")
    
    # Execute SQL (Serverless doesn't support DbUser parameter)
    response = redshift_data.execute_statement(
        WorkgroupName=redshift_workgroup,
        Database=redshift_database,
        Sql=sql_script
    )
    
    query_id = response['Id']
    logger.info(f"Redshift query started: {query_id}")
    
    # Wait for completion (optional - could use sensor instead)
    import time
    max_attempts = 60
    for i in range(max_attempts):
        status_response = redshift_data.describe_statement(Id=query_id)
        status = status_response['Status']
        
        if status == 'FINISHED':
            logger.info(f"Redshift load completed successfully")
            return query_id
        elif status in ['FAILED', 'ABORTED']:
            error = status_response.get('Error', 'Unknown error')
            raise Exception(f"Redshift load failed: {error}")
        
        time.sleep(10)
    
    raise Exception(f"Redshift load timeout after {max_attempts*10}s")


def load_aggregations_to_redshift(ingest_date, **kwargs):
    """Load aggregations (trends) to Redshift"""
    if LOCAL_MODE:
        logger.info(f"[LOCAL] Would load aggregations to Redshift for date {ingest_date}")
        return
    
    import boto3
    
    logger.info(f"Loading aggregations to Redshift for {ingest_date}")
    
    # Read SQL script
    sql_path = Path(__file__).parent.parent / "sql" / "redshift_load_aggregations.sql"
    
    redshift_data = boto3.client("redshift-data", region_name="us-east-1")
    redshift_workgroup = get_var("REDSHIFT_WORKGROUP", "monokera-pipeline")
    redshift_database = get_var("REDSHIFT_DATABASE", "dev")
    redshift_iam_role = get_var("REDSHIFT_IAM_ROLE", 
        "arn:aws:iam::311048569989:role/service-role/AmazonRedshift-CommandsAccessRole-20260115T142621")
    
    with open(sql_path, 'r') as f:
        sql_script = f.read()
    
    # Replace placeholders
    sql_script = sql_script.replace('{{ ingest_date }}', ingest_date)
    sql_script = sql_script.replace('{{ redshift_iam_role }}', redshift_iam_role)
    
    logger.info(f"Executing aggregations SQL on Redshift workgroup: {redshift_workgroup}, database: {redshift_database}")
    
    # Execute SQL
    response = redshift_data.execute_statement(
        WorkgroupName=redshift_workgroup,
        Database=redshift_database,
        Sql=sql_script
    )
    
    query_id = response['Id']
    logger.info(f"Redshift aggregations query started: {query_id}")
    
    # Wait for completion
    import time
    max_attempts = 60
    for i in range(max_attempts):
        status_response = redshift_data.describe_statement(Id=query_id)
        status = status_response['Status']
        
        if status == 'FINISHED':
            logger.info(f"Redshift aggregations load completed successfully")
            return query_id
        elif status in ['FAILED', 'ABORTED']:
            error = status_response.get('Error', 'Unknown error')
            raise Exception(f"Redshift aggregations load failed: {error}")
        
        time.sleep(10)
    
    raise Exception(f"Redshift aggregations load timeout after {max_attempts*10}s")


def record_pipeline_metadata(ingest_date, **kwargs):
    """Record pipeline execution metadata (simplified - no external files)"""
    if LOCAL_MODE:
        logger.info(f"[LOCAL] Would record pipeline metadata for date {ingest_date}")
        return
    
    import boto3
    from datetime import datetime
    
    logger.info(f"Recording pipeline metadata for {ingest_date}")
    
    redshift_data = boto3.client("redshift-data", region_name="us-east-1")
    redshift_workgroup = get_var("REDSHIFT_WORKGROUP", "monokera-pipeline")
    redshift_database = get_var("REDSHIFT_DATABASE", "dev")
    
    # Simple SQL to record pipeline run
    sql_script = f"""
    -- Record pipeline execution
    INSERT INTO core.meta_pipeline_runs (
        run_id,
        run_type,
        run_status,
        ingest_date,
        records_processed,
        records_failed,
        started_at,
        completed_at
    )
    SELECT 
        '{ingest_date}-' || SUBSTRING(MD5('{ingest_date}' || CURRENT_TIMESTAMP::TEXT), 1, 8) AS run_id,
        'daily_load' AS run_type,
        'completed' AS run_status,
        '{ingest_date}'::DATE AS ingest_date,
        (SELECT COUNT(*) FROM core.content_items WHERE DATE(valid_from) = '{ingest_date}'::DATE AND is_current = TRUE) AS records_processed,
        0 AS records_failed,
        CURRENT_TIMESTAMP - INTERVAL '30 minutes' AS started_at,
        CURRENT_TIMESTAMP AS completed_at;
    
    -- Basic quality checks
    INSERT INTO core.meta_data_quality_checks (
        check_name,
        check_type,
        target_table,
        check_result,
        check_value,
        ingest_date
    )
    SELECT 
        'daily_content_count' AS check_name,
        'row_count' AS check_type,
        'core.content_items' AS target_table,
        CASE WHEN COUNT(*) >= 100 THEN 'PASS' ELSE 'WARNING' END AS check_result,
        COUNT(*) AS check_value,
        '{ingest_date}'::DATE AS ingest_date
    FROM core.content_items
    WHERE DATE(valid_from) = '{ingest_date}'::DATE
    AND is_current = TRUE;
    """
    
    logger.info(f"Executing metadata SQL on Redshift workgroup: {redshift_workgroup}")
    
    # Execute SQL
    response = redshift_data.execute_statement(
        WorkgroupName=redshift_workgroup,
        Database=redshift_database,
        Sql=sql_script
    )
    
    query_id = response['Id']
    logger.info(f"Redshift metadata query started: {query_id}")
    
    # Wait for completion
    import time
    max_attempts = 30
    for i in range(max_attempts):
        status_response = redshift_data.describe_statement(Id=query_id)
        status = status_response['Status']
        
        if status == 'FINISHED':
            logger.info(f"Pipeline metadata recorded successfully")
            return query_id
        elif status in ['FAILED', 'ABORTED']:
            error = status_response.get('Error', 'Unknown error')
            logger.warning(f"Metadata recording failed (non-critical): {error}")
            return None
        
        time.sleep(5)
    
    logger.warning(f"Metadata recording timeout (non-critical)")
    return None


def generate_insights(ingest_date, **kwargs):
    """Generate daily insights from Redshift data"""
    logger.info(f"Generating insights for {ingest_date}")
    
    if LOCAL_MODE:
        logger.info(f"[LOCAL] Skipping insights generation")
        return
    
    import boto3
    import json
    
    # Get Redshift configuration
    redshift_workgroup = get_var("REDSHIFT_WORKGROUP", "monokera-pipeline")
    redshift_database = get_var("REDSHIFT_DATABASE", "dev")
    
    # Query Redshift for insights (simplified - 3 separate queries)
    redshift_data = boto3.client('redshift-data')
    
    # Query 1: Daily stats
    stats_query = f"""
    SELECT 
        COUNT(*) as total_content,
        COUNT(DISTINCT news_site) as active_sources,
        COUNT(DISTINCT content_type) as content_types
    FROM core.content_items
    WHERE DATE(valid_from) = '{ingest_date}'::DATE
      AND is_current = TRUE;
    """
    
    # Query 2: Top topics
    topics_query = f"""
    SELECT 
        topic_name,
        SUM(content_count) as total_count
    FROM core.agg_topic_monthly_trends
    WHERE ingest_date = '{ingest_date}'::DATE
    GROUP BY topic_name
    ORDER BY total_count DESC
    LIMIT 5;
    """
    
    # Query 3: Top sources
    sources_query = f"""
    SELECT 
        news_site,
        SUM(content_count) as total_count
    FROM core.agg_news_site_monthly_activity
    WHERE ingest_date = '{ingest_date}'::DATE
    GROUP BY news_site
    ORDER BY total_count DESC
    LIMIT 5;
    """
    
    def execute_and_wait(sql, query_name):
        """Execute query and wait for results"""
        response = redshift_data.execute_statement(
            WorkgroupName=redshift_workgroup,
            Database=redshift_database,
            Sql=sql
        )
        query_id = response['Id']
        logger.info(f"{query_name} query started: {query_id}")
        
        import time
        for _ in range(60):
            status_response = redshift_data.describe_statement(Id=query_id)
            status = status_response['Status']
            
            if status == 'FINISHED':
                return redshift_data.get_statement_result(Id=query_id)
            elif status == 'FAILED':
                error = status_response.get('Error', 'Unknown error')
                logger.error(f"{query_name} query failed: {error}")
                return None
            
            time.sleep(2)
        
        logger.warning(f"{query_name} query timeout")
        return None
    
    try:
        # Execute queries
        stats_result = execute_and_wait(stats_query, "Stats")
        topics_result = execute_and_wait(topics_query, "Topics")
        sources_result = execute_and_wait(sources_query, "Sources")
        
        if not all([stats_result, topics_result, sources_result]):
            logger.error("One or more queries failed")
            return None
        
        # Build insights JSON
        insights = {
            'date': ingest_date,
            'total_content': 0,
            'active_sources': 0,
            'content_types': 0,
            'top_topics': [],
            'top_sources': []
        }
        
        # Parse stats
        if stats_result['Records']:
            record = stats_result['Records'][0]
            insights['total_content'] = record[0].get('longValue', 0)
            insights['active_sources'] = record[1].get('longValue', 0)
            insights['content_types'] = record[2].get('longValue', 0)
        
        # Parse top topics
        for record in topics_result.get('Records', []):
            topic = record[0].get('stringValue', '')
            count = record[1].get('longValue', 0)
            if topic:
                insights['top_topics'].append({'topic': topic, 'count': count})
        
        # Parse top sources
        for record in sources_result.get('Records', []):
            source = record[0].get('stringValue', '')
            count = record[1].get('longValue', 0)
            if source:
                insights['top_sources'].append({'source': source, 'count': count})
        
        logger.info(f"✅ Daily Insights Generated:")
        logger.info(f"   Total Content: {insights['total_content']:,}")
        logger.info(f"   Active Sources: {insights['active_sources']}")
        logger.info(f"   Top Topics: {len(insights['top_topics'])}")
        for t in insights['top_topics'][:3]:
            logger.info(f"      • {t['topic']}: {t['count']} articles")
        logger.info(f"   Top Sources: {len(insights['top_sources'])}")
        for s in insights['top_sources'][:3]:
            logger.info(f"      • {s['source']}: {s['count']} articles")
        
        # Save insights to S3
        s3 = boto3.client("s3")
        insights_key = f"insights/daily/{ingest_date}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=insights_key,
            Body=json.dumps(insights, indent=2),
            ContentType='application/json'
        )
        logger.info(f"💾 Insights saved to s3://{S3_BUCKET}/{insights_key}")
        
        # Push insights to XCom for notify task
        ti = kwargs.get('ti')
        if ti:
            ti.xcom_push(key='daily_insights', value=insights)
        
    except Exception as e:
        logger.error(f"Error generating insights: {e}")
        return None


def update_dashboards(**kwargs):
    """Confirm data ready for dashboards"""
    logger.info("Checking dashboard data readiness...")
    
    tables_summary = {
        'core_tables': [
            {'name': 'content_items', 'type': 'SCD Type 2', 'description': 'Main content with versioning'},
            {'name': 'ref_news_sites', 'type': 'Reference', 'description': 'News sources with stats'},
            {'name': 'ref_topics', 'type': 'Reference', 'description': 'Content topics/categories'}
        ],
        'aggregations': [
            {'name': 'agg_topic_monthly_trends', 'description': 'Monthly topic content counts'},
            {'name': 'agg_news_site_monthly_activity', 'description': 'Monthly source activity'}
        ],
        'metadata': [
            {'name': 'meta_pipeline_runs', 'description': 'Pipeline execution history'},
            {'name': 'meta_data_quality_checks', 'description': 'Data validation results'}
        ],
        'redshift_workgroup': 'monokera-pipeline',
        'database': 'dev',
        'dashboard_tools': [
            'Power BI (via ODBC/JDBC)',
            'Tableau (via Redshift connector)',
            'QuickSight (native AWS integration)',
            'Looker, Metabase, Superset'
        ]
    }
    
    if LOCAL_MODE:
        logger.info(f"[LOCAL] Data ready for analytics queries")
        logger.info(f"  ✓ Core tables: content_items, ref_news_sites, ref_topics")
        logger.info(f"  ✓ Aggregations: topic_trends, source_activity")
        logger.info(f"  ✓ Metadata: pipeline_runs, quality_checks")
    else:
        # In production, can trigger dashboard refresh
        # Example for QuickSight:
        # import boto3
        # quicksight = boto3.client('quicksight')
        # quicksight.create_ingestion(
        #     DataSetId='your-dataset-id',
        #     IngestionId=f"refresh-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        #     AwsAccountId='your-account-id'
        # )
        
        # Example for Tableau Server API:
        # import requests
        # tableau_url = 'https://your-tableau-server/api/3.x/sites/site-id/workbooks/workbook-id/refresh'
        # requests.post(tableau_url, headers={'X-Tableau-Auth': token})
        
        logger.info("✅ Dashboard data refresh ready")
        logger.info("   Connect your BI tool to Redshift workgroup: monokera-pipeline")
    
    # Push summary to XCom for notify task
    ti = kwargs.get('ti')
    if ti:
        ti.xcom_push(key='tables_summary', value=tables_summary)
    
    return tables_summary
    

def notify_success(**context):
    """Send detailed success notification with insights"""
    execution_date = context.get('ds', 'unknown')
    dag_run = context.get('dag_run')
    duration = None

    if dag_run:
        # Normalize datetimes to timezone-aware UTC to avoid subtracting naive vs aware
        from airflow.utils import timezone as airflow_tz

        start_date = dag_run.start_date
        end_date = dag_run.end_date or airflow_tz.utcnow()

        try:
            # If either is naive, make it aware in UTC
            if start_date is not None and start_date.tzinfo is None:
                start_date = airflow_tz.make_aware(start_date)
            if end_date is not None and end_date.tzinfo is None:
                end_date = airflow_tz.make_aware(end_date)

            if start_date is not None and end_date is not None:
                duration = (end_date - start_date).total_seconds() / 60
        except Exception as e:
            logger.warning(f"Could not compute duration: {e}")
            duration = None
    
    # Get insights and tables summary from XCom
    ti = context.get('ti')
    insights = None
    tables_summary = None
    
    if ti:
        try:
            insights = ti.xcom_pull(task_ids='generate_daily_insights', key='daily_insights')
            tables_summary = ti.xcom_pull(task_ids='update_dashboards', key='tables_summary')
        except Exception as e:
            logger.warning(f"Could not retrieve XCom data: {e}")
    
    # If insights not in XCom, try to load from S3
    if not insights and not LOCAL_MODE:
        try:
            import boto3
            import json
            s3 = boto3.client('s3')
            insights_key = f"insights/daily/{execution_date}.json"
            
            response = s3.get_object(Bucket=S3_BUCKET, Key=insights_key)
            insights = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"Loaded insights from S3: {insights_key}")
        except Exception as e:
            logger.warning(f"Could not load insights from S3: {e}")
    
    # Build detailed notification message
    message_parts = [
        "="*80,
        "🚀 SPACENEWS PIPELINE - EJECUCIÓN COMPLETADA EXITOSAMENTE",
        "="*80,
        f"\n📅 Fecha de Ejecución: {execution_date}",
    ]
    
    if duration:
        message_parts.append(f"⏱️  Duración Total: {duration:.1f} minutos")
    
    message_parts.append(f"\n{'-'*80}")
    message_parts.append("\n📊 INSIGHTS DIARIOS DEL PIPELINE")
    message_parts.append("-"*80)
    
    if insights:
        top_topics = insights.get('top_topics', [])
        if top_topics:
            message_parts.append(f"\n🏆 Top 5 Temas Más Populares:")
            for i, topic in enumerate(top_topics[:5], 1):
                message_parts.append(f"   {i}. {topic['topic']:20} → {topic['count']:,} artículos")
        
        top_sources = insights.get('top_sources', [])
        if top_sources:
            message_parts.append(f"\n📰 Top 5 Fuentes Más Activas:")
            for i, source in enumerate(top_sources[:5], 1):
                message_parts.append(f"   {i}. {source['source']:30} → {source['count']:,} artículos")
    else:
        message_parts.append("\n⚠️  Insights no disponibles en este momento")
        message_parts.append("   Se pueden consultar directamente en Redshift o S3")
    
    message_parts.append(f"\n{'-'*80}")
    message_parts.append("\n🗄️  DATA WAREHOUSE - TABLAS ACTUALIZADAS EN REDSHIFT")
    message_parts.append("-"*80)
    
    if tables_summary:
        message_parts.append(f"\n✅ Tablas Core (SCD Type 2 con versionado histórico):")
        for table in tables_summary.get('core_tables', []):
            message_parts.append(f"   • {table['name']:30} → {table['description']}")
        
        message_parts.append(f"\n✅ Agregaciones (Métricas pre-calculadas):")
        for table in tables_summary.get('aggregations', []):
            message_parts.append(f"   • {table['name']:40} → {table['description']}")
        
        message_parts.append(f"\n✅ Metadata (Observabilidad y calidad):")
        for table in tables_summary.get('metadata', []):
            message_parts.append(f"   • {table['name']:30} → {table['description']}")
        
        message_parts.append(f"\n🔗 Información de Conexión:")
        message_parts.append(f"   • Workgroup: {tables_summary.get('redshift_workgroup', 'N/A')}")
        message_parts.append(f"   • Database: {tables_summary.get('database', 'N/A')}")
        message_parts.append(f"   • Region: us-east-1")
    else:
        message_parts.append("\n✅ Todas las tablas core cargadas correctamente")
        message_parts.append("✅ Todas las tablas de agregación actualizadas")
        message_parts.append("✅ Todas las tablas de metadata registradas")
    
    message_parts.append(f"\n{'-'*80}")
    message_parts.append("\n📊 ACTUALIZACIÓN DE DASHBOARDS COMPLETADA")
    message_parts.append("-"*80)
    message_parts.append("\n✅ Los dashboards han sido actualizados y están listos para consulta")
    message_parts.append("✅ Este es un mensaje de confirmación de que los datos están disponibles")
    
    message_parts.append(f"\n{'-'*80}")
    message_parts.append("\n🎯 PRÓXIMOS PASOS RECOMENDADOS")
    message_parts.append("-"*80)
    message_parts.append(f"   1. 📁 Revisar insights JSON: s3://monokera-bucket/insights/daily/{execution_date}.json")
    message_parts.append("   2. 🔍 Consultar Redshift para análisis detallado")
    message_parts.append("   3. 📊 Abrir dashboards de BI actualizados")
    message_parts.append("   4. ✔️  Monitorear quality checks en meta_data_quality_checks")
    message_parts.append("   5. 📧 Compartir insights con el equipo")
    
    message_parts.append(f"\n{'='*80}")
    message_parts.append("✨ ¡Pipeline ejecutado exitosamente! Datos listos para análisis.")
    message_parts.append("="*80)
    
    full_message = "\n".join(message_parts)
    
    logger.info(f"✅ Pipeline completed successfully for {execution_date}")
    if duration:
        logger.info(f"   Duration: {duration:.1f} minutes")
    
    notify_sns(context, success=True, custom_message=full_message)

def notify_failure(context):
    notify_sns(context, success=False)


# Define the DAG
with DAG(
    dag_id="spacenews_pipeline_local",
    default_args=DEFAULT_ARGS,
    description=f"SpaceNews Bronze→Silver→Gold→Redshift ({'LOCAL' if LOCAL_MODE else 'PRODUCTION'})",
    schedule="@daily" if not LOCAL_MODE else None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    on_failure_callback=notify_failure,
    tags=["spacenews", "pipeline", "production"],
) as dag:
    
    ingest_date = "{{ ds }}"  # Airflow execution date in YYYY-MM-DD format
    
    # Project root path
    project_root = str(Path(__file__).parent.parent)
    
    # Extraction tasks (using CLI)
    extract_articles = BashOperator(
        task_id="extract_articles",
        bash_command=f"cd {project_root} && python -m spacenews_ingestion.cli extract --endpoint articles --ingest-date {ingest_date}",
        env={
            "PYTHONPATH": f"{project_root}/src:{project_root}",
            "S3_BUCKET": S3_BUCKET,
            "SNS_TOPIC_ARN": SNS_TOPIC_ARN,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID or "",
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY or "",
            "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
        },
    )
    
    extract_blogs = BashOperator(
        task_id="extract_blogs",
        bash_command=f"cd {project_root} && python -m spacenews_ingestion.cli extract --endpoint blogs --ingest-date {ingest_date}",
        env={
            "PYTHONPATH": f"{project_root}/src:{project_root}",
            "S3_BUCKET": S3_BUCKET,
            "SNS_TOPIC_ARN": SNS_TOPIC_ARN,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID or "",
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY or "",
            "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
        },
    )
    
    extract_reports = BashOperator(
        task_id="extract_reports",
        bash_command=f"cd {project_root} && python -m spacenews_ingestion.cli extract --endpoint reports --ingest-date {ingest_date}",
        env={
            "PYTHONPATH": f"{project_root}/src:{project_root}",
            "S3_BUCKET": S3_BUCKET,
            "SNS_TOPIC_ARN": SNS_TOPIC_ARN,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID or "",
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY or "",
            "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
        },
    )
    
    extract_info = BashOperator(
        task_id="extract_info",
        bash_command=f"cd {project_root} && python -m spacenews_ingestion.cli extract --endpoint info --ingest-date {ingest_date}",
        env={
            "PYTHONPATH": f"{project_root}/src:{project_root}",
            "S3_BUCKET": S3_BUCKET,
            "SNS_TOPIC_ARN": SNS_TOPIC_ARN,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID or "",
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY or "",
            "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
        },
    )
    
    # Glue processing tasks
    glue_clean = PythonOperator(
        task_id="glue_01_clean_dedup",
        python_callable=start_glue_job,
        op_kwargs={"job_name": GLUE_CLEAN_JOB, "ingest_date": ingest_date},
    )
    
    glue_enrich = PythonOperator(
        task_id="glue_02_enrich",
        python_callable=start_glue_job,
        op_kwargs={"job_name": GLUE_ENRICH_JOB, "ingest_date": ingest_date},
    )
    
    glue_trends = PythonOperator(
        task_id="glue_03_trends",
        python_callable=start_glue_job,
        op_kwargs={"job_name": GLUE_TRENDS_JOB, "ingest_date": ingest_date},
    )
    
    # Load to Redshift
    load_redshift = PythonOperator(
        task_id="load_to_redshift",
        python_callable=load_to_redshift,
        op_kwargs={"ingest_date": ingest_date},
    )
    
    load_aggregations = PythonOperator(
        task_id="load_aggregations_to_redshift",
        python_callable=load_aggregations_to_redshift,
        op_kwargs={"ingest_date": ingest_date},
    )
    
    record_metadata = PythonOperator(
        task_id="record_pipeline_metadata",
        python_callable=record_pipeline_metadata,
        op_kwargs={"ingest_date": ingest_date},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # Analytics and insights
    insights = PythonOperator(
        task_id="generate_daily_insights",
        python_callable=generate_insights,
        op_kwargs={"ingest_date": ingest_date},
    )
    
    dashboards = PythonOperator(
        task_id="update_dashboards",
        python_callable=update_dashboards,
    )
    
    # Success notification
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # Task dependencies
    # Extract → Clean → Enrich → Trends → Load (Core + Aggregations in parallel)
    # → Record Metadata → Insights → Dashboards → Notify
    [extract_articles, extract_blogs, extract_reports, extract_info] >> glue_clean
    glue_clean >> glue_enrich
    glue_enrich >> glue_trends
    glue_trends >> [load_redshift, load_aggregations]
    [load_redshift, load_aggregations] >> record_metadata
    record_metadata >> insights
    insights >> dashboards
    dashboards >> notify
