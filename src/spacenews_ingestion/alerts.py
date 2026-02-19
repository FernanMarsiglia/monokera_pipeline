"""
SNS Alerting Module
Sends alerts for pipeline failures and critical events
"""

import json
import os
from typing import Dict, Any, Optional
from datetime import datetime

try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from .logging_config import get_logger

logger = get_logger(__name__)


class SNSAlerter:
    """
    SNS-based alerting system for pipeline events
    
    Sends alerts for:
    - Extraction failures
    - Spark job failures
    - Data quality issues
    - Threshold breaches
    """
    
    def __init__(
        self,
        topic_arn: Optional[str] = None,
        region_name: str = "us-east-1",
        dry_run: bool = False
    ):
        """
        Initialize SNS alerter
        
        Args:
            topic_arn: SNS topic ARN for alerts (defaults to SNS_TOPIC_ARN env var)
            region_name: AWS region (defaults to AWS_REGION env var or us-east-1)
            dry_run: If True, log alerts instead of sending (defaults to ALERT_DRY_RUN env var)
        """
        # Get from environment if not provided
        self.topic_arn = topic_arn or os.environ.get("SNS_TOPIC_ARN")
        self.region_name = region_name or os.environ.get("AWS_REGION", "us-east-1")
        self.dry_run = dry_run or os.environ.get("ALERT_DRY_RUN", "false").lower() == "true"
        self.sns_client = None
        
        if not dry_run and topic_arn and BOTO3_AVAILABLE:
            try:
                self.sns_client = boto3.client("sns", region_name=region_name)
                logger.info(
                    "Initialized SNS alerter",
                    extra={"topic_arn": topic_arn, "region": region_name}
                )
            except Exception as e:
                logger.warning(
                    "Failed to initialize SNS client",
                    extra={"error": str(e)}
                )
        elif not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, alerts will be logged only")
    
    def send_alert(
        self,
        subject: str,
        message: str,
        severity: str = "WARNING",
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send an alert via SNS
        
        Args:
            subject: Alert subject line
            message: Alert message body
            severity: Alert severity (INFO, WARNING, ERROR, CRITICAL)
            metadata: Additional context metadata
        
        Returns:
            True if alert sent successfully, False otherwise
        """
        metadata = metadata or {}
        
        # Build alert payload
        alert_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "severity": severity,
            "subject": subject,
            "alert_message": message,  # Renamed to avoid LogRecord conflict
            "metadata": metadata
        }
        
        # Log alert
        logger.warning(
            f"ALERT: {subject}",
            extra={
                "alert_severity": severity,
                "alert_message": message,
                **metadata
            }
        )
        
        # If dry run or no SNS client, just log
        if self.dry_run or not self.sns_client or not self.topic_arn:
            logger.info("Alert logged (not sent to SNS)", extra=alert_data)
            return True
        
        # Send to SNS
        try:
            response = self.sns_client.publish(
                TopicArn=self.topic_arn,
                Subject=f"[{severity}] {subject}",
                Message=json.dumps(alert_data, indent=2),
                MessageAttributes={
                    "severity": {
                        "DataType": "String",
                        "StringValue": severity
                    }
                }
            )
            
            logger.info(
                "Alert sent to SNS",
                extra={
                    "message_id": response.get("MessageId"),
                    "subject": subject,
                    "severity": severity
                }
            )
            return True
        
        except ClientError as e:
            logger.error(
                "Failed to send SNS alert",
                extra={
                    "error": str(e),
                    "subject": subject,
                    "severity": severity
                }
            )
            return False
    
    def alert_extraction_failure(
        self,
        endpoint: str,
        error: Exception,
        run_id: str,
        records_fetched: int = 0
    ):
        """Alert for extraction failures"""
        self.send_alert(
            subject=f"Extraction Failed: {endpoint}",
            message=f"Failed to extract data from {endpoint}. Error: {str(error)}",
            severity="ERROR",
            metadata={
                "endpoint": endpoint,
                "run_id": run_id,
                "records_fetched": records_fetched,
                "error_type": type(error).__name__,
                "error_message": str(error)
            }
        )
    
    def alert_glue_job_failure(
        self,
        job_name: str,
        job_run_id: str,
        error_message: str
    ):
        """Alert for Glue job failures"""
        self.send_alert(
            subject=f"Glue Job Failed: {job_name}",
            message=f"Glue job {job_name} failed. Run ID: {job_run_id}. Error: {error_message}",
            severity="ERROR",
            metadata={
                "job_name": job_name,
                "job_run_id": job_run_id,
                "error_message": error_message
            }
        )
    
    def alert_data_quality_issue(
        self,
        issue_type: str,
        description: str,
        affected_records: int,
        total_records: int,
        ingest_date: str
    ):
        """Alert for data quality issues"""
        failure_rate = (affected_records / total_records * 100) if total_records > 0 else 0
        
        severity = "CRITICAL" if failure_rate > 50 else "WARNING"
        
        self.send_alert(
            subject=f"Data Quality Issue: {issue_type}",
            message=f"{description}. Affected: {affected_records}/{total_records} ({failure_rate:.2f}%)",
            severity=severity,
            metadata={
                "issue_type": issue_type,
                "affected_records": affected_records,
                "total_records": total_records,
                "failure_rate": failure_rate,
                "ingest_date": ingest_date
            }
        )
    
    def alert_threshold_breach(
        self,
        metric_name: str,
        current_value: float,
        threshold: float,
        comparison: str = ">"
    ):
        """Alert for metric threshold breaches"""
        self.send_alert(
            subject=f"Threshold Breach: {metric_name}",
            message=f"Metric {metric_name} breached threshold. Current: {current_value}, Threshold: {threshold}",
            severity="WARNING",
            metadata={
                "metric_name": metric_name,
                "current_value": current_value,
                "threshold": threshold,
                "comparison": comparison
            }
        )
    
    def alert_pipeline_success(
        self,
        run_id: str,
        records_processed: int,
        duration_seconds: float,
        ingest_date: str
    ):
        """Alert for successful pipeline completion"""
        self.send_alert(
            subject="Pipeline Completed Successfully",
            message=f"Pipeline completed. Processed {records_processed} records in {duration_seconds:.2f}s",
            severity="INFO",
            metadata={
                "run_id": run_id,
                "records_processed": records_processed,
                "duration_seconds": duration_seconds,
                "ingest_date": ingest_date
            }
        )


# Example usage
if __name__ == "__main__":
    # Dry run (logging only)
    alerter = SNSAlerter(dry_run=True)
    
    # Test different alert types
    alerter.alert_extraction_failure(
        endpoint="articles",
        error=Exception("Connection timeout"),
        run_id="test_run_123",
        records_fetched=50
    )
    
    alerter.alert_data_quality_issue(
        issue_type="MissingRequiredField",
        description="Records missing 'title' field",
        affected_records=10,
        total_records=200,
        ingest_date="2026-02-16"
    )
    
    alerter.alert_pipeline_success(
        run_id="test_run_123",
        records_processed=600,
        duration_seconds=125.5,
        ingest_date="2026-02-16"
    )
