import json
import boto3
import urllib.parse
import http.client
import time
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    # Initialize MWAA client
    mwaa_client = boto3.client("mwaa")
    environment_name = "Lab3-MWAA"
    dag_id = "music_streaming_pipeline"

    try:
        # Process S3 event
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
            if not key.endswith(".csv"):  # Ignore non-CSV files
                print(f"Skipping non-CSV file: s3://{bucket}/{key}")
                continue

            print(f"Processing S3 event: s3://{bucket}/{key}")

            # Get MWAA CLI token with retry
            for attempt in range(3):
                try:
                    response = mwaa_client.create_cli_token(Name=environment_name)
                    cli_token = response["CliToken"]
                    web_server_hostname = response["WebServerHostname"]
                    break
                except ClientError as e:
                    if attempt < 2:
                        time.sleep(2**attempt)  # Exponential backoff
                        continue
                    raise e

            # Trigger DAG via MWAA REST API
            conn = http.client.HTTPSConnection(web_server_hostname)
            headers = {
                "Authorization": f"Bearer {cli_token}",
                "Content-Type": "application/json",
            }
            payload = {
                "dag_run_id": f's3_trigger_{key.replace("/", "_")}_{int(context.get_remaining_time_in_millis())}',
                "conf": {"s3_key": key},
            }

            for attempt in range(3):
                try:
                    conn.request(
                        "POST",
                        f"/api/v1/dags/{dag_id}/dagRuns",
                        body=json.dumps(payload),
                        headers=headers,
                    )
                    response = conn.getresponse()
                    if response.status == 200:
                        print(
                            f"Successfully triggered DAG {dag_id} for s3://{bucket}/{key}"
                        )
                        break
                    else:
                        print(
                            f"Failed to trigger DAG: {response.status} {response.reason}"
                        )
                        if attempt < 2:
                            time.sleep(2**attempt)
                            continue
                        return {
                            "statusCode": response.status,
                            "body": json.dumps({"error": response.reason}),
                        }
                except Exception as e:
                    if attempt < 2:
                        time.sleep(2**attempt)
                        continue
                    raise e
                finally:
                    conn.close()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "DAG triggered successfully"}),
        }

    except Exception as e:
        print(f"Error triggering DAG: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
