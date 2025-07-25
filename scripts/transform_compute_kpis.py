from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    countDistinct,
    sum,
    lit,
    trim,
    concat,
    struct,
    collect_list,
    to_json,
    rank,
)
from pyspark.sql.window import Window
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_key", "bucket"])
spark = SparkSession.builder.appName(
    f"{args['JOB_NAME']}-Transformation-Multi-Table"
).getOrCreate()


glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["bucket"]
s3_key = args["s3_key"]
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("FileValidationMetadata")


def get_valid_files(prefix):
    try:
        response = table.scan(
            FilterExpression="begins_with(file_key, :prefix) AND validation_status = :status",
            ExpressionAttributeValues={":prefix": prefix, ":status": "SUCCESS"},
        )
        return [item["file_key"] for item in response.get("Items", [])]
    except ClientError as e:
        print(f"Error scanning DynamoDB for {prefix}: {str(e)}")
        raise e


def main():
    try:
        # --- 1. Read the new validated streams file ---
        new_streams_df = spark.read.csv(
            f"s3://{bucket}/{s3_key}",
            header=True,
            inferSchema=True,
        ).withColumn("listen_time", to_date(col("listen_time")))

        new_streams_df = new_streams_df.dropna(
            subset=["user_id", "track_id", "listen_time"]
        ).dropDuplicates(["user_id", "track_id", "listen_time"])

        # --- 2. Upsert/Merge into the Hudi fact table ---
        fact_path = f"s3://{bucket}/data/fact/streams/"
        hudi_options = {
            "hoodie.table.name": "streams_fact",
            "hoodie.datasource.write.recordkey.field": "user_id,track_id,listen_time",
            "hoodie.datasource.write.partitionpath.field": "listen_time",
            "hoodie.datasource.write.table.name": "streams_fact",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.precombine.field": "listen_time",
            "hoodie.upsert.shuffle.parallelism": 2,
            "hoodie.insert.shuffle.parallelism": 2,
            "hoodie.datasource.write.hive_style_partitioning": "true",
        }

        new_streams_df.write.format("hudi").options(**hudi_options).mode("append").save(
            fact_path
        )

        # --- 3. Read all streams for the relevant date(s) from the Hudi fact table ---
        listen_dates = [
            row["listen_time"]
            for row in new_streams_df.select("listen_time").distinct().collect()
        ]
        if not listen_dates:
            print("No valid listen_time found in new streams file.")
            job.commit()
            exit(0)

        # Hudi supports partition pruning, so we can read only the relevant partitions
        fact_paths = [f"{fact_path}/listen_time={d}" for d in listen_dates]
        streams_df = spark.read.format("hudi").load(*fact_paths)
        streams_df = streams_df.dropDuplicates(["user_id", "track_id", "listen_time"])

        # --- 4. Read and clean songs and users as before ---
        songs_keys = get_valid_files("data/raw/songs/")
        users_keys = get_valid_files("data/raw/users/")
        if not (songs_keys and users_keys):
            print("No valid files for all required types, exiting")
            job.commit()
            exit(0)
        songs_df = spark.read.csv(
            [f"s3://{bucket}/{key}" for key in songs_keys],
            header=True,
            inferSchema=True,
        )
        users_df = spark.read.csv(
            [f"s3://{bucket}/{key}" for key in users_keys],
            header=True,
            inferSchema=True,
        )

        songs_df = (
            songs_df.withColumn("duration_ms", col("duration_ms").cast("integer"))
            .dropna(subset=["duration_ms"])
            .filter(
                col("track_id").isNotNull()
                & col("track_genre").isNotNull()
                & (trim(col("track_genre")) != "")
                & ~col("track_genre").rlike("^[0-9\\.]+$")
            )
            .dropDuplicates(["track_id"])
        )
        users_df = users_df.dropna(subset=["user_id"]).dropDuplicates(["user_id"])

        # --- 5. Join and compute KPIs as before ---
        joined_df = (
            streams_df.join(songs_df, "track_id", "inner")
            .join(users_df, "user_id", "inner")
            .withColumn("listen_date", to_date(col("listen_time")))
        )
        output_date = datetime.utcnow().strftime("%Y/%m/%d")

        # 1. Daily Genre-Level KPIs
        genre_stats_agg = (
            joined_df.groupBy("listen_date", "track_genre")
            .agg(
                count("*").alias("listen_count"),
                countDistinct("user_id").alias("unique_listeners"),
                sum("duration_ms").alias("total_listening_time_ms"),
            )
            .withColumn(
                "avg_listening_time_per_user_ms",
                col("total_listening_time_ms") / col("unique_listeners"),
            )
        )
        genre_stats_flat = genre_stats_agg.select(
            col("listen_date").cast("string"),
            concat(lit("GENRE_"), col("track_genre")).alias("kpi_type"),
            col("track_genre"),
            col("listen_count"),
            col("unique_listeners"),
            col("total_listening_time_ms"),
            col("avg_listening_time_per_user_ms"),
            lit(datetime.utcnow().isoformat()).alias("ingestion_timestamp"),
        )
        # 2. Top 3 Songs per Genre
        song_counts = joined_df.groupBy("listen_date", "track_genre", "track_name").agg(
            count("*").alias("song_listen_count")
        )
        window_songs = Window.partitionBy("listen_date", "track_genre").orderBy(
            col("song_listen_count").desc()
        )
        top_songs_ranked = song_counts.withColumn(
            "rank", rank().over(window_songs)
        ).filter(col("rank") <= 3)
        top_songs = (
            top_songs_ranked.groupBy("listen_date", "track_genre")
            .agg(
                collect_list(
                    struct(col("rank"), col("track_name"), col("song_listen_count"))
                ).alias("top_songs_list")
            )
            .select(
                col("listen_date").cast("string"),
                col("track_genre"),
                to_json(col("top_songs_list")).alias("top_songs_data"),
                lit(datetime.utcnow().isoformat()).alias("ingestion_timestamp"),
            )
        )
        # 3. Top 5 Genres per Day
        genre_counts = joined_df.groupBy("listen_date", "track_genre").agg(
            count("*").alias("genre_listen_count")
        )
        window_genres = Window.partitionBy("listen_date").orderBy(
            col("genre_listen_count").desc()
        )
        top_genres_ranked = genre_counts.withColumn(
            "rank", rank().over(window_genres)
        ).filter(col("rank") <= 5)
        top_genres = (
            top_genres_ranked.groupBy("listen_date")
            .agg(
                collect_list(
                    struct(col("rank"), col("track_genre"), col("genre_listen_count"))
                ).alias("top_genres_list")
            )
            .select(
                col("listen_date").cast("string"),
                to_json(col("top_genres_list")).alias("top_genres_data"),
                lit(datetime.utcnow().isoformat()).alias("ingestion_timestamp"),
            )
        )
        # --- Save KPIs to S3 ---
        kpi_base_path = f"s3://{bucket}/data/kpis/{output_date}/"
        genre_stats_flat.write.mode("overwrite").partitionBy("listen_date").parquet(
            f"{kpi_base_path}genre_stats/"
        )
        top_songs.write.mode("overwrite").partitionBy("listen_date").parquet(
            f"{kpi_base_path}top_songs/"
        )
        top_genres.write.mode("overwrite").partitionBy("listen_date").parquet(
            f"{kpi_base_path}top_genres/"
        )
        print(f"Saved all KPIs to base path: {kpi_base_path}")
    except Exception as e:
        print(f"Error in transformation/KPI computation: {str(e)}")
        raise e
    finally:
        job.commit()


if __name__ == "__main__":
    main()
