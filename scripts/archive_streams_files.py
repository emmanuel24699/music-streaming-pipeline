import sys
import boto3
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from datetime import datetime
import logging

# Configure logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_key", "bucket"])
bucket = args["bucket"]
s3_key = args["s3_key"]

# Initialize boto3 clients
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
metadata_table = dynamodb.Table("FileValidationMetadata")


def update_metadata(status, message):
    """Updates the archival status in the metadata table."""
    try:
        metadata_table.update_item(
            Key={"file_key": s3_key},
            UpdateExpression="SET archival_status = :status, archival_message = :msg, archival_timestamp = :ts",
            ExpressionAttributeValues={
                ":status": status,
                ":msg": message,
                ":ts": datetime.utcnow().isoformat(),
            },
        )
        logger.info(f"Successfully updated metadata for {s3_key} with status: {status}")
    except ClientError as e:
        logger.error(f"Failed to update DynamoDB metadata for {s3_key}: {e}")
        raise


def main():
    """Main function to archive a processed S3 file."""
    try:
        # 1. Idempotency Check: Verify the file has not already been archived
        response = metadata_table.get_item(Key={"file_key": s3_key})
        item = response.get("Item", {})
        if item.get("archival_status") == "SUCCESS":
            logger.info(f"File {s3_key} has already been archived. Skipping.")
            return

        # 2. Construct Destination Path
        now = datetime.utcnow()
        date_path = now.strftime("%Y/%m/%d")
        timestamp_prefix = now.strftime("%Y%m%d%H%M%S")
        original_filename = s3_key.split("/")[-1]

        destination_key = (
            f"data/archive/{date_path}/{timestamp_prefix}_{original_filename}"
        )

        logger.info(f"Archiving file from s3://{bucket}/{s3_key}")
        logger.info(f"Destination: s3://{bucket}/{destination_key}")

        # 3. S3 Copy Operation
        copy_source = {"Bucket": bucket, "Key": s3_key}
        s3_client.copy_object(
            CopySource=copy_source, Bucket=bucket, Key=destination_key
        )
        logger.info("File copied to archive successfully.")

        # 4. S3 Delete Operation (from original location)
        s3_client.delete_object(Bucket=bucket, Key=s3_key)
        logger.info("Original file deleted successfully.")

        # 5. Update Metadata on Success
        success_message = f"File successfully archived to {destination_key}"
        update_metadata("SUCCESS", success_message)

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "NoSuchKey":
            error_message = f"Source file not found: s3://{bucket}/{s3_key}. It may have been archived by a previous run."
            logger.warning(error_message)
            # Mark as success to prevent DAG from failing on reruns
            update_metadata("SUCCESS", error_message)
        else:
            error_message = f"An AWS error occurred during archival: {e}"
            logger.error(error_message)
            update_metadata("FAILED", error_message)
            raise e

    except Exception as e:
        error_message = f"A general error occurred: {str(e)}"
        logger.error(error_message)
        update_metadata("FAILED", error_message)
        raise e


if __name__ == "__main__":
    main()
