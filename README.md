# Music Streaming Data Pipeline with AWS Glue and MWAA

## Overview

This project implements a real-time ETL pipeline on AWS to process music streaming data. It ingests CSV files (`streams`, `songs`, `users`) from S3, validates their structure, computes key performance indicators (KPIs), stores KPIs in DynamoDB, and archives processed `streams` files. The pipeline is orchestrated using Airflow (MWAA) and triggered by S3 events via Lambda.

### Key Features

- **Ingestion**: Lambda triggers the pipeline for new `streams` files in `s3://lab3-music-streaming-amalitechde1/data/raw/streams/`.
- **Validation**: Ensures required columns in input files, moving invalid files to `data/error/`.
- **Transformation**: Computes KPIs (daily genre stats, top 3 songs per genre, top 5 genres per day).
- **Storage**: Ingests KPIs into DynamoDB.
- **Archival**: Moves processed `streams` files to `data/archive/YYYY/MM/DD/` with timestamp prefixes.
- **Orchestration**: Airflow DAG ensures sequential execution with retries and short-circuiting for invalid files.
- **Metadata**: Tracks file status in `FileValidationMetadata` DynamoDB table.

## Architecture

```
[S3 Upload: streams.csv]
       |
[Lambda Trigger]
       |
[Airflow DAG: music_streaming_pipeline]
       |
[1. Validate Files (Glue: validate_input_files)]
       |
[2. Check Valid Files (Python: check_valid_files)]
       |
[3. Transform & Compute KPIs (Glue: transform_compute_kpis)]
       |
[4. Load KPIs to DynamoDB (Glue: load_kpis_to_dynamodb)]
       |
[5. Archive Streams File (Glue: archive_streams_files)]
       |
[DynamoDB: FileValidationMetadata, KPIs]
[S3: data/kpis/, data/archive/]
```

### Components

- **AWS Lambda**: `lambda_function.py` triggers the Airflow DAG for new `streams` files.
- **AWS Glue**: Python Shell and Spark jobs for validation, transformation, ingestion, and archival.
- **Amazon MWAA (Airflow)**: Orchestrates tasks with `music_streaming_pipeline.py`.
- **Amazon S3**: Stores input files (`data/raw/`), KPIs (`data/kpis/`), archived files (`data/archive/`), and scripts (`scripts/`, `dags/`).
- **Amazon DynamoDB**: Stores metadata (`FileValidationMetadata`) and KPIs.
- **Amazon CloudWatch**: Logs job execution and errors.

## Prerequisites

- **AWS Account**: Active account.
- **IAM Role**: `GlueTransformKPIsRole` with permissions for S3, DynamoDB, Glue, and MWAA.
- **S3 Bucket**: `lab3-music-streaming-amalitechde1` with folders:
  - `data/raw/streams/`, `data/raw/songs/`, `data/raw/users/`
  - `data/kpis/`, `data/archive/`, `data/error/`
  - `scripts/`, `dags/`
- **DynamoDB Tables**:
  - `FileValidationMetadata` (partition key: `file_key`, string).
  - `MusicStreamingKPIs` (partition key: `listen_date`, sort key: `kpi_type`).
  - `TopSongsByGenre` (partition key: `listen_date`, sort key: `track_genre`).
  - `TopGenresByDay` (partition key: `listen_date`).
- **MWAA Environment**: `Lab3-MWAA` with Airflow 2.x, Python 3.9+, and AWS plugins.

### IAM Policy for `GlueTransformKPIsRole`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": ["arn:aws:s3:::lab3-music-streaming-amalitechde1/*"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:Scan"
        "dynamodb:Query"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:<account-id>:table/FileValidationMetadata",
        "arn:aws:dynamodb:us-east-1:<account-id>:table/MusicStreamingKPIs",
        "arn:aws:dynamodb:us-east-1:<account-id>:table/TopSongsByGenre",
        "arn:aws:dynamodb:us-east-1:<account-id>:table/TopGenresByDay"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["glue:StartJobRun", "glue:GetJobRun"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "*"
    }
  ]
}
```

## Setup Instructions

### 1. Create S3 Bucket and Folders

1. Go to **S3** > **Create bucket** > Name: `lab3-music-streaming-amalitechde1`, Region: `us-east-1`.
2. Create folders:
   - `data/raw/streams/`, `data/raw/songs/`, `data/raw/users/`
   - `data/kpis/`, `data/archive/`, `data/error/`
   - `scripts/`, `dags/`

### 2. Create DynamoDB Tables

1. Go to **DynamoDB** > **Create table**:
   - Table: `FileValidationMetadata`, Partition key: `file_key` (String), On-demand capacity.
   - Table: `MusicStreamingKPIs`, Partition key: `listen_date` (String), Sort key: `kpi_type` (String).
   - Table: `TopSongsByGenre`, Partition key: `listen_date` (String), Sort key: `track_genre` (String).
   - Table: `TopGenresByDay`, Partition key: `listen_date` (String).
2. Confirm tables are active.

### 3. Set Up IAM Role

1. Go to **IAM** > **Roles** > **Create role** > Type: AWS Glue.
2. Attach inline policy (see above).
3. Name: `GlueTransformKPIsRole`.

### 4. Deploy Lambda Function

1. Go to **S3** > `lab3-music-streaming-amalitechde1` > `scripts/` > Upload `lambda_function.py`.
2. Go to **Lambda** > **Create function**:
   - Name: `trigger_music_pipeline_dag`, Runtime: Python 3.9.
   - Role: Create a role with `AmazonMWAAFullAccess`, `AmazonS3ReadOnlyAccess`, and CloudWatch logs permissions.
   - Code: Copy `lambda_function.py`.
3. Add S3 trigger:
   - Event type: `s3:ObjectCreated:*`, Prefix: `data/raw/streams/`, Suffix: `.csv`.
4. Deploy and test.

### 5. Deploy Glue Jobs

1. Upload scripts to **S3** > `lab3-music-streaming-amalitechde1` > `scripts/`:
   - `validate_input_files.py`
   - `transform_compute_kpis.py`
   - `load_kpis_to_dynamodb.py`
   - `archive_streams_files.py`
2. Go to **AWS Glue** > **Jobs** > **Add job** for each:
   - **validate_input_files**: Python Shell, Script: `validate_input_files.py`, Role: `GlueTransformKPIsRole`.
   - **transform_compute_kpis**: Spark, Script: `transform_compute_kpis.py`, Role: `GlueTransformKPIsRole`, `--bucket lab3-music-streaming-amalitechde1`.
   - **load_kpis_to_dynamodb**: Spark, Script: `load_kpis_to_dynamodb.py`, Role: `GlueTransformKPIsRole`.
   - **archive_streams_files**: Python Shell, Script: `archive_streams_files.py`, Role: `GlueTransformKPIsRole`, `--bucket lab3-music-streaming-amalitechde1`.

### 6. Deploy Airflow DAG

1. Upload `music_streaming_pipeline.py` to **S3** > `lab3-music-streaming-amalitechde1` > `dags/`.
2. Go to **Airflow** > `Lab3-MWAA` > **Web UI** > **DAGs** > Verify `music_streaming_pipeline` is active.

## Usage

1. **Trigger Pipeline**:
   - Upload a `streams` CSV file to `s3://lab3-music-streaming-amalitechde1/data/raw/streams/` (e.g., `streams1.csv`).
   - Ensure valid `songs` and `users` CSV files exist in `data/raw/songs/` and `data/raw/users/`.
2. **Monitor Execution**:
   - **Airflow**: Go to **Airflow Web UI** > `music_streaming_pipeline` > **Graph** or **Tree** view.
   - **Glue**: Check job runs in **AWS Glue** > **Jobs**.
   - **CloudWatch**: View logs in `/aws/glue/<job_name>` and `/aws/lambda/trigger_music_pipeline_dag`.
3. **Verify Output**:
   - **S3**: KPIs in `data/kpis/YYYY/MM/DD/`, archived files in `data/archive/YYYY/MM/DD/`.
   - **DynamoDB**: Query `FileValidationMetadata` for `validation_status`, `kpi_ingestion_status`, `archival_status`. Check KPI tables for data.

## Testing

1. **Upload Test Files**:
   - Valid `streams1.csv`:
     ```csv
     user_id,track_id,listen_time
     u1,t1,2025-06-22 10:00:00
     u2,t2,2025-06-22 10:01:00
     ```
   - Valid `songs1.csv`:
     ```csv
     track_id,track_name,track_genre,duration_ms
     t1,Song1,Pop,180000
     t2,Song2,Rock,200000
     ```
   - Valid `users1.csv`:
     ```csv
     user_id,user_name
     u1,User1
     u2,User2
     ```
   - Invalid `streams2.csv` (missing `track_id`):
     ```csv
     user_id,listen_time
     u1,2025-06-22 10:00:00
     ```
2. **Run Pipeline**:
   - Upload files to respective S3 folders.
   - Monitor DAG in Airflow.
3. **Expected Results**:
   - Valid files: KPIs in `data/kpis/`, DynamoDB tables populated, `streams1.csv` archived to `data/archive/YYYY/MM/DD/<timestamp>_streams1.csv`, `FileValidationMetadata` updated (`SUCCESS`).
   - Invalid files: Moved to `data/error/`, `FileValidationMetadata` updated (`FAILED`).

## Maintenance

- **Monitoring**:
  - **Airflow**: Check DAG runs for failures or retries (15 retries, 1-minute delay).
  - **CloudWatch**: Monitor logs for errors in Glue jobs and Lambda.
  - **DynamoDB**: Query `FileValidationMetadata` for `FAILED` statuses.
- **Troubleshooting**:
  - **Glue Job Fails**: Check CloudWatch logs, verify IAM permissions, ensure S3 paths exist.
  - **DAG Fails**: Inspect Airflow logs.
  - **Lambda Fails**: Verify MWAA VPC access, check CloudWatch logs.
- **Scaling**:
  - Increase DynamoDB capacity for high write throughput.
  - Adjust Glue job concurrency if processing multiple files.

## Future Improvements

- Implement SNS notifications for job failures or archival errors.
- Enhance logging with custom CloudWatch metrics (e.g., file processing time).

##
