from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, isnull, length, lit
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import sys
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_key", "bucket"])
spark = SparkSession.builder.appName(
    f"{args['JOB_NAME']}-Ingestion-Multi-Table"
).getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["bucket"]
s3_key = args["s3_key"]
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
metadata_table = dynamodb.Table("FileValidationMetadata")

# Table names
GENRE_STATS_TABLE = "MusicStreamingKPIs"
TOP_SONGS_TABLE = "TopSongsByGenre"
TOP_GENRES_TABLE = "TopGenresByDay"


def update_metadata(s3_key, status, message):
    try:
        metadata_table.update_item(
            Key={"file_key": s3_key},
            UpdateExpression="SET kpi_ingestion_status = :status, kpi_ingestion_message = :msg, kpi_ingestion_timestamp = :ts",
            ExpressionAttributeValues={
                ":status": status,
                ":msg": message,
                ":ts": datetime.utcnow().isoformat(),
            },
        )
        print(f"Updated metadata for {s3_key}: {status}")
    except ClientError as e:
        print(f"Error updating metadata for {s3_key}: {str(e)}")
        raise e


def write_to_dynamo(df, table_name, pk_mapping, sk_mapping=None):
    """Helper function to write a DataFrame to a DynamoDB table."""
    if df.rdd.isEmpty():
        print(f"No data to write for table {table_name}. Skipping.")
        return 0

    kpis_dynamic = DynamicFrame.fromDF(df, glueContext, f"{table_name}_dynamic")

    specs = [(pk_mapping, "cast:string")]
    if sk_mapping:
        specs.append((sk_mapping, "cast:string"))
    kpis_resolved = kpis_dynamic.resolveChoice(specs=specs)

    # Set up connection options
    connection_options = {
        "dynamodb.output.tableName": table_name,
        "dynamodb.throughput.write.percentage": "1.0",
        f"dynamodb.mapping.pk": pk_mapping,
    }
    if sk_mapping:
        connection_options[f"dynamodb.mapping.sk"] = sk_mapping

    print(f"Writing to DynamoDB table: {table_name}")
    glueContext.write_dynamic_frame.from_options(
        frame=kpis_resolved,
        connection_type="dynamodb",
        connection_options=connection_options,
    )
    count = kpis_resolved.count()
    print(f"Successfully wrote {count} records to {table_name}.")
    return count


def main():
    try:
        # Extract listen_date from the source streams file
        streams_df = spark.read.csv(
            f"s3://{bucket}/{s3_key}", header=True, inferSchema=True
        ).filter(~isnull(col("listen_time")))
        listen_date_df = streams_df.select(
            to_date(col("listen_time")).alias("listen_date")
        ).distinct()
        if listen_date_df.rdd.isEmpty():
            raise ValueError("No valid 'listen_time' values found.")
        listen_date = listen_date_df.collect()[0]["listen_date"]
        if listen_date is None:
            raise ValueError("Extracted listen_date is null.")
        listen_date_str = listen_date.strftime("%Y-%m-%d")

        # Base path for all processed KPIs for the day
        output_date = datetime.utcnow().strftime("%Y/%m/%d")
        kpi_base_path = f"s3://{bucket}/data/kpis/{output_date}/"

        total_records_ingested = 0

        # --- 1. Ingest Genre Stats ---
        genre_stats_path = f"{kpi_base_path}genre_stats/listen_date={listen_date_str}"
        genre_stats_df = spark.read.parquet(genre_stats_path).withColumn(
            "listen_date", lit(listen_date_str)
        )
        total_records_ingested += write_to_dynamo(
            genre_stats_df,
            GENRE_STATS_TABLE,
            pk_mapping="listen_date",
            sk_mapping="kpi_type",
        )

        # --- 2. Ingest Top Songs ---
        top_songs_path = f"{kpi_base_path}top_songs/listen_date={listen_date_str}"
        top_songs_df = spark.read.parquet(top_songs_path).withColumn(
            "listen_date", lit(listen_date_str)
        )
        total_records_ingested += write_to_dynamo(
            top_songs_df,
            TOP_SONGS_TABLE,
            pk_mapping="listen_date",
            sk_mapping="track_genre",
        )

        # --- 3. Ingest Top Genres ---
        top_genres_path = f"{kpi_base_path}top_genres/listen_date={listen_date_str}"
        top_genres_df = spark.read.parquet(top_genres_path).withColumn(
            "listen_date", lit(listen_date_str)
        )
        total_records_ingested += write_to_dynamo(
            top_genres_df, TOP_GENRES_TABLE, pk_mapping="listen_date"
        )

        # Update metadata on overall success
        success_msg = f"Successfully ingested a total of {total_records_ingested} KPI records into DynamoDB for {listen_date_str}."
        update_metadata(s3_key, "SUCCESS", success_msg)

    except Exception as e:
        error_msg = f"Error in KPI ingestion job: {str(e)}"
        print(error_msg)
        update_metadata(s3_key, "FAILED", error_msg)
        raise e
    finally:
        job.commit()


if __name__ == "__main__":
    main()
