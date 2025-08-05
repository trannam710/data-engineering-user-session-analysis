import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark():

    spark = SparkSession.builder.appName("DailyBatchJob").getOrCreate()

    return spark

def create_aggregate_tables(spark, yesterday):

    # Read data from parquet
    yesterday_data_path = f"file:///opt/spark/data_lake/sessions/date={yesterday}"
    print(f"Reading data from: {yesterday_data_path}")
    raw_data = spark.read.format("parquet").load(yesterday_data_path)

    # Create daily user activity table
    daily_user_activity = raw_data.withColumn("session_start_date", to_date(col("session_start_time"))) \
                                    .groupBy(col("user_id"), col("session_start_time")) \
                                    .agg(
                                        count(col("session_id")).alias("num_sessions"),
                                        sum(col("session_duration_seconds").alias("total_duration"))    
                                    ) \
                                    .select(
                                        "user_id",
                                        "session_start_time",
                                        "num_sessions",
                                        "total_duration"
                                    )
    
    # Create daily funnel analysis

    event_schema = StructType(
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("url", StringType(), True),
        StructField("utm_source", StringType(), True)
    )

    daily_funnel_analysis = raw_data.withColumn("events_list", from_json(col("event_json"), event_schema)) \
                                    .withColumn("event", explode(col("event_list"))) \
                                    .withColumn("session_start_date", to_date(col("session_start_time"))) \
                                    .groupBy(col("session_start_date"), col("event.event_type").alias("step_name")) \
                                    .agg(
                                        count_distinct(col("user_id").alias("users_count"))
                                    ) \
                                    .select(
                                        "step_name",
                                        "users_count"
                                    )
    
    # Create daily traffic source report

    daily_traffic_source = raw_data.withColumn("session_start_date", to_date(col("session_start_time"))) \
                                    .groupBy(col("session_start_date"), col("landing_utm_source")) \
                                    .agg(
                                        count_distinct(col("user_id").alias("users_count")),
                                        avg(col("session_duration_seconds"))
                                    ) \
                                    .select(
                                        "landing_utm_source",
                                        "users_count",
                                        "session_duration_seconds"
                                    )
    
    return daily_user_activity, daily_funnel_analysis, daily_traffic_source

def write_to_postgres(df, table_name):

    df.write.jdbc(
        url="jdbc:postgresql://localhost:5432/airflow",
        table=table_name,
        mode="overwrite",
        property={"user":"airflow", "password":"airflow", "driver": "org.postgresql.Driver"}
    )

    print(f"Data for table {table_name} written successfully.")

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: spark-submit spark_batch_job.py <date_string>")
        sys.exit(-1)

    spark = create_spark()
    yesterday_date = sys.argv[1]

    daily_activity, daily_funnel, daily_traffic = create_aggregate_tables(spark, yesterday_date)

    # Write data to postgre
    write_to_postgres(daily_activity, "daily_user_activity")
    write_to_postgres(daily_funnel, "daily_funnel_analysis")
    write_to_postgres(daily_traffic, "daily_traffic_source_report")
    
    spark.stop()