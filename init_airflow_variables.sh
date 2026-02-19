#!/bin/bash
# Script para cargar todas las variables de Airflow desde .env
# Ejecutar dentro del contenedor: docker exec airflow-webserver-1 bash /opt/airflow/init_airflow_variables.sh

set -e

echo "🚀 Cargando variables de Airflow..."

# Cargar .env si existe
if [ -f /opt/airflow/.env ]; then
    source /opt/airflow/.env
fi

# AWS Credentials
airflow variables set AWS_ACCESS_KEY_ID "${AWS_ACCESS_KEY_ID}" || echo "⚠️  AWS_ACCESS_KEY_ID no configurado"
airflow variables set AWS_SECRET_ACCESS_KEY "${AWS_SECRET_ACCESS_KEY}" || echo "⚠️  AWS_SECRET_ACCESS_KEY no configurado"
airflow variables set AWS_DEFAULT_REGION "${AWS_DEFAULT_REGION:-us-east-1}"

# S3 Configuration
airflow variables set S3_BUCKET "${SPACENEWS_S3_BUCKET:-monokera-bucket}"
airflow variables set BRONZE_PREFIX "data/bronze"
airflow variables set SILVER_PREFIX "data/silver"
airflow variables set GOLD_PREFIX "data/gold"

# Glue Jobs
airflow variables set GLUE_JOB_CLEAN_DEDUP "spacenews-01-clean-dedup"
airflow variables set GLUE_JOB_ENRICH "spacenews-02-enrich-topics-entities"
airflow variables set GLUE_JOB_TRENDS "spacenews-03-trends-aggregations"

# SNS
airflow variables set SNS_TOPIC_ARN "arn:aws:sns:us-east-1:311048569989:spacenews-pipeline-alerts"

# Redshift Configuration
airflow variables set REDSHIFT_WORKGROUP "monokera-pipeline"
airflow variables set REDSHIFT_DATABASE "${REDSHIFT_DB:-dev}"
airflow variables set REDSHIFT_IAM_ROLE "arn:aws:iam::311048569989:role/service-role/AmazonRedshift-CommandsAccessRole-20260115T142621"

# Pipeline Mode
airflow variables set LOCAL_MODE "false"

echo "✅ Variables cargadas exitosamente"
echo ""
echo "📋 Verificar variables:"
airflow variables list
