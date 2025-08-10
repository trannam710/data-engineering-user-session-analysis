import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark():
    spark = SparkSession.builder \
                        .appName("DailyBatchJob") \
                        .config("spark.cores.max", "1") \
                        .config("spark.executor.memory", "512m") \
                        .getOrCreate()
    return spark

def create_aggregate_tables(spark, yesterday):
    # Reads data from the Parquet files for the given date
    yesterday_data_path = f"file:///opt/spark/data_lake/sessions/date={yesterday}"
    print(f"Reading data from: {yesterday_data_path}")
    
    try:
        raw_data = spark.read.format("parquet").load(yesterday_data_path)
    except Exception as e:
        print(f"Error reading data: {e}")
        return None, None, None

    # Creates the Daily User Activity table
    daily_user_activity = raw_data.withColumn("session_start_date", to_date(col("session_start_time"))) \
                                    .groupBy(col("user_id"), col("session_start_date")) \
                                    .agg(
                                        count(col("session_id")).alias("num_sessions"),
                                        sum(col("session_duration_seconds")).alias("total_duration")
                                    ) \
                                    .select(
                                        "user_id",
                                        "session_start_date",
                                        "num_sessions",
                                        "total_duration"
                                    )

    # Creates the Daily Funnel Analysis table
    event_schema = ArrayType(StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("url", StringType(), True),
        StructField("utm_source", StringType(), True)
    ]))

    daily_funnel_analysis = raw_data.withColumn("events_list", from_json(col("events_json"), event_schema)) \
                                    .withColumn("event", explode(col("events_list"))) \
                                    .withColumn("session_start_date", to_date(col("session_start_time"))) \
                                    .groupBy(col("session_start_date"), col("event.event_type").alias("step_name")) \
                                    .agg(
                                        count_distinct(col("user_id")).alias("users_count")
                                    ) \
                                    .select(
                                        "session_start_date",
                                        "step_name",
                                        "users_count"
                                    )

    # Creates the Daily Traffic Source Report table
    daily_traffic_source = raw_data.withColumn("session_start_date", to_date(col("session_start_time"))) \
                                    .groupBy(col("session_start_date"), col("landing_utm_source")) \
                                    .agg(
                                        count_distinct(col("user_id")).alias("users_count"),
                                        avg(col("session_duration_seconds")).alias("avg_duration")
                                    ) \
                                    .select(
                                        "session_start_date",
                                        "landing_utm_source",
                                        "users_count",
                                        "avg_duration"
                                    )
    
    return daily_user_activity, daily_funnel_analysis, daily_traffic_source

def write_to_postgres(df, table_name):

    # Writes the DataFrame to PostgreSQL

    df.write.jdbc(
        url="jdbc:postgresql://postgres:5432/airflow",
        table=table_name,
        mode="overwrite",
        properties={"user":"airflow", "password":"airflow", "driver": "org.postgresql.Driver"}
    )
    print(f"Data for table {table_name} written successfully.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit spark_batch_job.py <date_string>")
        sys.exit(-1)
        
    spark = create_spark()
    yesterday_date = sys.argv[1]
    
    daily_activity, daily_funnel, daily_traffic = create_aggregate_tables(spark, yesterday_date)
    
    if daily_activity and daily_funnel and daily_traffic:
        # Writes the data to Postgres
        write_to_postgres(daily_activity, "daily_user_activity")
        write_to_postgres(daily_funnel, "daily_funnel_analysis")
        write_to_postgres(daily_traffic, "daily_traffic_source_report")
    
    spark.stop()