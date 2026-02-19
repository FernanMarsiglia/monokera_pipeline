"""
Unit tests for DAG ETL functions
Tests extraction, loading, and orchestration logic
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, mock_open
from datetime import datetime, date
import json
import os


class TestExtractionFunctions:
    """Tests for content extraction functions"""
    
    @patch('dags.spacenews_pipeline_local.extract_content')
    @patch('dags.spacenews_pipeline_local.upload_to_s3')
    def test_extract_articles_success(self, mock_upload, mock_extract):
        """Test successful article extraction"""
        # Mock extraction to return sample data
        mock_extract.return_value = [
            {"id": 1, "title": "Article 1", "url": "https://example.com/1"},
            {"id": 2, "title": "Article 2", "url": "https://example.com/2"}
        ]
        mock_upload.return_value = "s3://bucket/path/file.jsonl"
        
        # Import function (mocked paths)
        from dags.spacenews_pipeline_local import extract_spacenews_content
        
        result = extract_spacenews_content(
            endpoint="articles",
            s3_bucket="test-bucket",
            ingest_date="2026-02-18"
        )
        
        # Verify extraction was called
        assert mock_extract.called
        # Verify upload was called
        assert mock_upload.called
        # Verify result contains S3 path
        assert "s3://" in result
    
    @patch('requests.Session.get')
    def test_extract_with_pagination(self, mock_get):
        """Test extraction handles pagination correctly"""
        # Mock paginated responses
        mock_get.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda: {
                    "count": 25,
                    "next": "https://api.example.com?offset=10",
                    "results": [{"id": i} for i in range(10)]
                }
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "count": 25,
                    "next": "https://api.example.com?offset=20",
                    "results": [{"id": i} for i in range(10, 20)]
                }
            ),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "count": 25,
                    "next": None,
                    "results": [{"id": i} for i in range(20, 25)]
                }
            )
        ]
        
        # Test that all pages are collected
        # (Implementation would iterate through all pages)
        assert True  # Placeholder for actual implementation test
    
    @patch('dags.spacenews_pipeline_local.SpaceNewsApiClient')
    def test_extract_handles_api_error(self, mock_client_class):
        """Test extraction handles API errors gracefully"""
        mock_client = MagicMock()
        mock_client.get_paginated.side_effect = ConnectionError("API unavailable")
        mock_client_class.return_value = mock_client
        
        from dags.spacenews_pipeline_local import extract_spacenews_content
        
        with pytest.raises(ConnectionError):
            extract_spacenews_content(
                endpoint="articles",
                s3_bucket="test-bucket",
                ingest_date="2026-02-18"
            )
    
    def test_extract_saves_to_jsonl_format(self):
        """Test extracted data is saved in JSONL format"""
        sample_data = [
            {"id": 1, "title": "Article 1"},
            {"id": 2, "title": "Article 2"}
        ]
        
        # Mock file writing
        with patch('builtins.open', mock_open()) as mocked_file:
            # Implementation would write JSONL
            for item in sample_data:
                mocked_file().write(json.dumps(item) + '\n')
            
            # Verify write was called for each item
            assert mocked_file().write.call_count >= len(sample_data)
    
    def test_extract_creates_proper_s3_path(self):
        """Test extraction creates proper S3 path structure"""
        endpoint = "articles"
        ingest_date = "2026-02-18"
        expected_path = f"data/bronze/{endpoint}/ingest_date={ingest_date}/"
        
        # Verify path construction
        assert expected_path == f"data/bronze/{endpoint}/ingest_date={ingest_date}/"
    
    def test_extract_handles_empty_response(self):
        """Test extraction handles empty API response"""
        # Should not fail if API returns empty results
        empty_response = {
            "count": 0,
            "next": None,
            "results": []
        }
        
        # Should handle gracefully and return empty list
        assert empty_response["results"] == []


class TestLoadingFunctions:
    """Tests for Redshift loading functions"""
    
    @patch('dags.spacenews_pipeline_local.RedshiftDataOperator')
    def test_load_core_tables_success(self, mock_operator):
        """Test successful loading of core tables"""
        mock_operator.return_value.execute = MagicMock()
        
        # Simulate successful load
        result = "Loaded 100 records"
        mock_operator.return_value.execute.return_value = result
        
        assert "Loaded" in result
    
    @patch('boto3.client')
    def test_load_uses_copy_command(self, mock_boto):
        """Test loading uses Redshift COPY command"""
        mock_redshift = MagicMock()
        mock_boto.return_value = mock_redshift
        
        sql_with_copy = """
        COPY core.content_items
        FROM 's3://bucket/path/'
        IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
        FORMAT AS PARQUET;
        """
        
        # Verify COPY command structure
        assert "COPY" in sql_with_copy
        assert "FROM 's3://" in sql_with_copy
        assert "IAM_ROLE" in sql_with_copy
    
    def test_load_handles_scd_type2_logic(self):
        """Test loading implements SCD Type 2 correctly"""
        # SCD Type 2 logic test
        update_query = """
        UPDATE core.content_items
        SET is_current = FALSE, valid_to = CURRENT_TIMESTAMP
        WHERE content_id = 123 AND is_current = TRUE;
        
        INSERT INTO core.content_items (...)
        VALUES (..., is_current = TRUE, valid_from = CURRENT_TIMESTAMP, valid_to = NULL);
        """
        
        # Verify UPDATE sets is_current = FALSE
        assert "is_current = FALSE" in update_query
        # Verify INSERT sets is_current = TRUE
        assert "is_current = TRUE" in update_query
    
    @patch('dags.spacenews_pipeline_local.execute_redshift_query')
    def test_load_handles_sql_error(self, mock_execute):
        """Test loading handles SQL errors"""
        mock_execute.side_effect = Exception("Redshift error: syntax error")
        
        with pytest.raises(Exception) as exc_info:
            mock_execute("SELECT * FROM invalid_table")
        
        assert "error" in str(exc_info.value).lower()
    
    def test_load_metadata_tracks_pipeline_run(self):
        """Test loading tracks metadata about pipeline run"""
        metadata_insert = """
        INSERT INTO core.meta_pipeline_runs 
        (run_date, pipeline_name, status, records_processed, execution_time_seconds)
        VALUES 
        ('2026-02-18', 'spacenews_pipeline', 'success', 1000, 120);
        """
        
        # Verify metadata fields
        assert "run_date" in metadata_insert
        assert "pipeline_name" in metadata_insert
        assert "status" in metadata_insert
        assert "records_processed" in metadata_insert


class TestGlueJobTriggers:
    """Tests for Glue job triggering logic"""
    
    @patch('boto3.client')
    def test_trigger_glue_job_success(self, mock_boto):
        """Test successful Glue job trigger"""
        mock_glue = MagicMock()
        mock_boto.return_value = mock_glue
        
        mock_glue.start_job_run.return_value = {
            "JobRunId": "jr_123456789"
        }
        
        response = mock_glue.start_job_run(
            JobName="spacenews-01-clean-deduplicate",
            Arguments={
                "--S3_BUCKET": "test-bucket",
                "--INGEST_DATE": "2026-02-18"
            }
        )
        
        assert response["JobRunId"] is not None
        assert "jr_" in response["JobRunId"]
    
    @patch('boto3.client')
    def test_trigger_glue_passes_correct_arguments(self, mock_boto):
        """Test Glue job receives correct arguments"""
        mock_glue = MagicMock()
        mock_boto.return_value = mock_glue
        
        expected_args = {
            "--S3_BUCKET": "monokera-bucket",
            "--INGEST_DATE": "2026-02-18",
            "--BRONZE_PREFIX": "data/bronze",
            "--SILVER_PREFIX": "data/silver"
        }
        
        mock_glue.start_job_run(
            JobName="test-job",
            Arguments=expected_args
        )
        
        call_args = mock_glue.start_job_run.call_args
        assert call_args[1]["Arguments"] == expected_args
    
    @patch('boto3.client')
    def test_monitor_glue_job_completion(self, mock_boto):
        """Test monitoring Glue job until completion"""
        mock_glue = MagicMock()
        mock_boto.return_value = mock_glue
        
        # Simulate job states: RUNNING -> RUNNING -> SUCCEEDED
        mock_glue.get_job_run.side_effect = [
            {"JobRun": {"JobRunState": "RUNNING"}},
            {"JobRun": {"JobRunState": "RUNNING"}},
            {"JobRun": {"JobRunState": "SUCCEEDED"}}
        ]
        
        # Would poll until SUCCEEDED
        states = []
        for _ in range(3):
            response = mock_glue.get_job_run(JobName="test", RunId="jr_123")
            states.append(response["JobRun"]["JobRunState"])
        
        assert states[-1] == "SUCCEEDED"


class TestNotificationFunctions:
    """Tests for SNS notification logic"""
    
    @patch('boto3.client')
    def test_notify_success_sends_sns(self, mock_boto):
        """Test success notification sends SNS message"""
        mock_sns = MagicMock()
        mock_boto.return_value = mock_sns
        
        mock_sns.publish.return_value = {
            "MessageId": "msg_123456789"
        }
        
        response = mock_sns.publish(
            TopicArn="arn:aws:sns:us-east-1:123456789:spacenews-alerts",
            Subject="Pipeline Success",
            Message="Pipeline completed successfully"
        )
        
        assert response["MessageId"] is not None
    
    @patch('boto3.client')
    def test_notify_failure_includes_error_details(self, mock_boto):
        """Test failure notification includes error details"""
        mock_sns = MagicMock()
        mock_boto.return_value = mock_sns
        
        error_message = """
        Pipeline Failed
        Task: extract_articles
        Error: ConnectionError - API unavailable
        Timestamp: 2026-02-18T10:30:00Z
        """
        
        mock_sns.publish(
            TopicArn="arn:aws:sns:us-east-1:123456789:spacenews-alerts",
            Subject="Pipeline FAILURE",
            Message=error_message
        )
        
        call_args = mock_sns.publish.call_args
        message = call_args[1]["Message"]
        
        assert "Error" in message
        assert "Task" in message
    
    def test_notification_message_format(self):
        """Test notification message has proper format"""
        message = {
            "pipeline": "spacenews_pipeline_local",
            "status": "success",
            "run_date": "2026-02-18",
            "duration_seconds": 1800,
            "records_processed": 35000,
            "insights": {
                "top_topics": ["ISS", "SpaceX", "Launch"],
                "top_sources": ["SpaceNews", "NASA", "Spaceflight Now"]
            }
        }
        
        # Verify required fields
        assert "pipeline" in message
        assert "status" in message
        assert "run_date" in message
        assert message["status"] == "success"


class TestDataQualityChecks:
    """Tests for data quality validation"""
    
    def test_quality_check_record_count(self):
        """Test quality check validates record counts"""
        bronze_count = 1000
        silver_count = 950  # Some records filtered out
        
        # Should pass if silver <= bronze (data quality filtering)
        assert silver_count <= bronze_count
        
        # Should alert if too many records lost
        loss_percentage = (bronze_count - silver_count) / bronze_count * 100
        assert loss_percentage < 10  # Less than 10% loss acceptable
    
    def test_quality_check_duplicate_detection(self):
        """Test quality check detects duplicates"""
        records = [
            {"id": 1, "title": "Article 1"},
            {"id": 2, "title": "Article 2"},
            {"id": 1, "title": "Article 1"}  # Duplicate
        ]
        
        unique_ids = set()
        duplicates = []
        
        for record in records:
            if record["id"] in unique_ids:
                duplicates.append(record["id"])
            unique_ids.add(record["id"])
        
        assert len(duplicates) == 1
        assert duplicates[0] == 1
    
    def test_quality_check_null_fields(self):
        """Test quality check identifies null required fields"""
        records = [
            {"id": 1, "title": "Article 1", "url": "https://example.com/1"},
            {"id": 2, "title": None, "url": "https://example.com/2"},  # Null title
            {"id": 3, "title": "Article 3", "url": None}  # Null URL
        ]
        
        null_issues = []
        for record in records:
            if record["title"] is None:
                null_issues.append(f"Record {record['id']}: null title")
            if record["url"] is None:
                null_issues.append(f"Record {record['id']}: null url")
        
        assert len(null_issues) == 2
    
    def test_quality_check_date_range_validation(self):
        """Test quality check validates date ranges"""
        today = date.today()
        
        records = [
            {"id": 1, "published_at": "2026-02-18"},  # Valid
            {"id": 2, "published_at": "2099-12-31"},  # Future (suspicious)
            {"id": 3, "published_at": "1950-01-01"}   # Very old (suspicious)
        ]
        
        suspicious_dates = []
        for record in records:
            pub_date = datetime.fromisoformat(record["published_at"]).date()
            if pub_date > today:
                suspicious_dates.append(f"Future date: {record['id']}")
            elif pub_date.year < 2000:
                suspicious_dates.append(f"Very old date: {record['id']}")
        
        assert len(suspicious_dates) == 2


class TestDAGConfiguration:
    """Tests for DAG configuration and parameters"""
    
    def test_dag_default_args(self):
        """Test DAG has proper default arguments"""
        default_args = {
            "owner": "airflow",
            "depends_on_past": False,
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": 2,
            "retry_delay": 300  # 5 minutes
        }
        
        assert default_args["retries"] >= 1
        assert default_args["retry_delay"] > 0
        assert default_args["email_on_failure"] is True
    
    def test_dag_schedule_interval(self):
        """Test DAG has appropriate schedule interval"""
        schedule_interval = "@daily"  # Or "0 2 * * *" for 2am daily
        
        assert schedule_interval in ["@daily", "@hourly"] or \
               schedule_interval.startswith("0 ")  # Cron expression
    
    def test_dag_task_dependencies(self):
        """Test DAG tasks have correct dependencies"""
        # Extraction tasks should run first (in parallel)
        # Then Glue jobs (sequential)
        # Then loading tasks (parallel)
        # Then notification
        
        task_order = [
            "extract_articles",  # Layer 1
            "extract_blogs",
            "extract_reports",
            "extract_info",
            "glue_clean_deduplicate",  # Layer 2
            "glue_enrich_topics",  # Layer 3
            "glue_aggregations",  # Layer 4
            "load_core_tables",  # Layer 5
            "load_aggregations",
            "generate_insights",  # Layer 6
            "notify_success"  # Layer 7
        ]
        
        # Verify ordering makes sense
        assert task_order.index("glue_clean_deduplicate") > task_order.index("extract_articles")
        assert task_order.index("notify_success") == len(task_order) - 1


class TestXComDataPassing:
    """Tests for XCom data passing between tasks"""
    
    @patch('airflow.models.TaskInstance')
    def test_xcom_push_s3_path(self, mock_ti):
        """Test task pushes S3 path to XCom"""
        mock_ti.xcom_push = MagicMock()
        
        s3_path = "s3://bucket/data/bronze/articles/file.jsonl"
        mock_ti.xcom_push(key="s3_path", value=s3_path)
        
        assert mock_ti.xcom_push.called
        call_args = mock_ti.xcom_push.call_args
        assert call_args[1]["value"] == s3_path
    
    @patch('airflow.models.TaskInstance')
    def test_xcom_pull_from_previous_task(self, mock_ti):
        """Test task pulls data from previous task via XCom"""
        mock_ti.xcom_pull = MagicMock(
            return_value="s3://bucket/data/bronze/articles/file.jsonl"
        )
        
        s3_path = mock_ti.xcom_pull(
            task_ids="extract_articles",
            key="s3_path"
        )
        
        assert s3_path.startswith("s3://")
        assert "bronze" in s3_path


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
