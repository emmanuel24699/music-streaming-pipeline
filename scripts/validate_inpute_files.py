import boto3
import pandas as pd
import io
from datetime import datetime
from botocore.exceptions import ClientError

# Initialize AWS clients
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("FileValidationMetadata")
bucket = "lab3-music-streaming-amalitechde1"

# Validation rules (required columns only)
STREAMS_COLUMNS = ["user_id", "track_id", "listen_time"]
SONGS_COLUMNS = ["track_id", "track_name", "track_genre", "duration_ms"]
USERS_COLUMNS = ["user_id", "user_name"]


def validate_columns(df, required_cols, file_key):
    """Validate file for required columns only."""
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return False, f"Missing columns: {', '.join(missing_cols)}"
    return True, "Validation successful"


def move_to_error(file_key, error_message):
    """Move invalid file to error folder with timestamp prefix."""
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    file_name = file_key.split("/")[-1]
    error_key = f"data/error/{timestamp}_{file_name}"

    try:
        s3_client.copy_object(
            Bucket=bucket, CopySource={"Bucket": bucket, "Key": file_key}, Key=error_key
        )
        s3_client.delete_object(Bucket=bucket, Key=file_key)
        print(f"Moved {file_key} to {error_key}")
    except ClientError as e:
        print(f"Error moving {file_key}: {str(e)}")
        raise e

    return error_key


def write_metadata(file_key, file_type, status, message):
    """Write validation metadata to DynamoDB."""
    try:
        table.put_item(
            Item={
                "file_key": file_key,
                "file_type": file_type,
                "validation_status": status,
                "validation_timestamp": datetime.utcnow().isoformat(),
                "error_message": message if status == "FAILED" else "",
            }
        )
        print(f"Wrote metadata for {file_key} to DynamoDB")
    except ClientError as e:
        print(f"Error writing metadata for {file_key}: {str(e)}")
        raise e


def validate_file(file_key, file_type):
    """Validate a single file based on its type."""
    print(f"Validating {file_key} ({file_type})")

    try:
        # Read CSV file from S3
        obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))

        # Select required columns based on file type
        columns_map = {
            "streams": STREAMS_COLUMNS,
            "songs": SONGS_COLUMNS,
            "users": USERS_COLUMNS,
        }
        required_cols = columns_map[file_type]

        # Validate columns
        is_valid, message = validate_columns(df, required_cols, file_key)

        if not is_valid:
            error_key = move_to_error(file_key, message)
            write_metadata(file_key, file_type, "FAILED", message)
            print(f"Validation failed for {file_key}: {message}")
        else:
            write_metadata(file_key, file_type, "SUCCESS", message)
            print(f"Validation passed for {file_key}")

    except Exception as e:
        error_message = f"Error processing {file_key}: {str(e)}"
        move_to_error(file_key, error_message)
        write_metadata(file_key, file_type, "FAILED", error_message)
        print(error_message)


def main():
    """Main function to validate files in S3 prefixes."""
    prefixes = [
        ("data/raw/streams/", "streams"),
        ("data/raw/songs/", "songs"),
        ("data/raw/users/", "users"),
    ]

    for prefix, file_type in prefixes:
        try:
            # List files in prefix
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if "Contents" not in response:
                print(f"No files found in {prefix}")
                continue

            for obj in response["Contents"]:
                file_key = obj["Key"]
                if file_key.endswith(".csv"):
                    validate_file(file_key, file_type)

        except ClientError as e:
            print(f"Error listing files in {prefix}: {str(e)}")
            raise e


if __name__ == "__main__":
    main()
