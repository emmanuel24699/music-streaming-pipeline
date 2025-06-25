from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator
import boto3
from botocore.exceptions import ClientError
import logging

# DAG default arguments with exponential backoff
default_args = {
    "owner": "music_pipeline",
    "depends_on_past": False,
    "retries": 15,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=80),
}


def check_valid_files(**context):
    """Check if valid files exist in FileValidationMetadata."""
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("FileValidationMetadata")
    s3_key = context["dag_run"].conf.get("s3_key", "")

    if not s3_key:
        print("No s3_key provided in dag_run.conf")
        return False

    # Check specific streams file
    response = table.get_item(Key={"file_key": s3_key})
    streams_valid = (
        "Item" in response and response["Item"]["validation_status"] == "SUCCESS"
    )
    if not streams_valid:
        print(f"Streams file {s3_key} is not valid or not found")

    # Check for any valid songs files
    response = table.scan(
        FilterExpression="begins_with(file_key, :prefix) AND validation_status = :status",
        ExpressionAttributeValues={
            ":prefix": "data/raw/songs/",
            ":status": "SUCCESS",
        },
    )
    songs_valid = len(response.get("Items", [])) > 0
    if not songs_valid:
        print("No valid songs files found")

    # Check for any valid users files
    response = table.scan(
        FilterExpression="begins_with(file_key, :prefix) AND validation_status = :status",
        ExpressionAttributeValues={
            ":prefix": "data/raw/users/",
            ":status": "SUCCESS",
        },
    )
    users_valid = len(response.get("Items", [])) > 0
    if not users_valid:
        print("No valid users files found")

    # Return True only if all three are valid
    result = streams_valid and songs_valid and users_valid
    print(
        f"Valid files check: {'Proceeding' if result else 'Skipping downstream tasks'}"
    )
    return result


def shortcircuit_handler(**context):
    """
    Perform cleanup for invalid or missing files. This could include deleting or archiving invalid files from S3.
    """
    s3_key = context["dag_run"].conf.get("s3_key", "")
    bucket = "lab3-music-streaming-amalitechde1"
    s3 = boto3.client("s3", region_name="us-east-1")
    logger = logging.getLogger("airflow.task")

    if not s3_key:
        logger.info("No s3_key provided, nothing to clean up.")
        return

    # Example: Move the invalid file to an 'invalid/' prefix in the same bucket
    invalid_key = f"invalid/{s3_key.split('/')[-1]}"
    try:
        logger.info(
            f"Archiving invalid file {s3_key} to {invalid_key} in bucket {bucket}"
        )
        s3.copy_object(
            Bucket=bucket, CopySource={"Bucket": bucket, "Key": s3_key}, Key=invalid_key
        )
        s3.delete_object(Bucket=bucket, Key=s3_key)
        logger.info(f"File {s3_key} archived and deleted from original location.")
    except ClientError as e:
        logger.error(f"Failed to archive/delete invalid file {s3_key}: {e}")


with DAG(
    dag_id="music_streaming_pipeline",
    default_args=default_args,
    description="Orchestrates music streaming data pipeline",
    schedule_interval=None,
    start_date=datetime(2025, 6, 19),
    catchup=False,
) as dag:

    # Task 1: Validate files
    validate_files = GlueJobOperator(
        task_id="validate_files",
        job_name="validate_input_files",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        retries=15,
    )

    # Task 2: Check for valid files
    check_valid_files_task = ShortCircuitOperator(
        task_id="check_valid_files",
        python_callable=check_valid_files,
        provide_context=True,
    )

    # Task 3: Clean and transform data
    transform_data = GlueJobOperator(
        task_id="transform_data",
        job_name="transform_compute_kpis",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        script_args={
            "--s3_key": "{{ dag_run.conf.s3_key }}",
            "--bucket": "lab3-music-streaming-amalitechde1",
        },
        retries=15,
    )

    # Task 4: Load to DynamoDB
    load_to_dynamodb = GlueJobOperator(
        task_id="load_to_dynamodb",
        job_name="load_kpis_to_dynamodb",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        script_args={
            "--s3_key": "{{ dag_run.conf.s3_key }}",
            "--bucket": "lab3-music-streaming-amalitechde1",
        },
        retries=15,
    )

    # Task 5: Archive the processed streams file
    archive_file = GlueJobOperator(
        task_id="archive_processed_file",
        job_name="archive_streams_files",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        script_args={
            "--JOB_NAME": "archive_streams_files",
            "--s3_key": "{{ dag_run.conf.s3_key }}",
            "--bucket": "lab3-music-streaming-amalitechde1",
        },
        retries=15,
    )

    # Shortcircuit/fail branch handler
    shortcircuit_handler_task = PythonOperator(
        task_id="shortcircuit_handler",
        python_callable=shortcircuit_handler,
        provide_context=True,
    )

    # Dependencies
    validate_files >> check_valid_files_task
    check_valid_files_task >> transform_data >> load_to_dynamodb >> archive_file
    check_valid_files_task >> shortcircuit_handler_task
