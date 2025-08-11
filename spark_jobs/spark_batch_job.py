import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

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
    
    # Create Session Level DataFrame
    session_level_df = raw_data.groupBy(
        "user_session"
    ).agg(
        (last("event_time") - first("event_time")).alias("session_duration_seconds"),
        count("event_type").alias("number_of_events"),
        countDistinct("product_id").alias("item_view_in_session"),
        # is purchase event
        max(when(col("event_type") == "purchase", 1).otherwise(0)).alias("is_purchase")
    )

    # Create User Level DataFrame
    user_level_df = raw_data.groupBy(
        "user_id"
    ).agg(
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
        sum(when(col("event_type") == "purchase") & (col("price").isNotNull()), col("price")).alias("total_spent"),
        max(when(col("event_type") == "purchase", col("event_time"))).alias("last_purchase_date"),
    )

    # Create ranked category for each user

    user_category_count = raw_data.filter(col("category_code").isNotNull()) \
                                .groupBy("user_id", "category_code") \
                                .agg(count("*").alias("interaction_count"))

    window_spec = Window.partitionBy("user_id").orderBy(col("interaction_count").desc())
    user_category_ranked = user_category_count.withColumn(
        "rank", row_number().over(window_spec)
    ).filter(col("rank") == 1).select(
        "user_id", "category_code"
    ).withColumnRenamed("category_code", "favorite_category")

    # Join user and rank level data and calculate date since last purchase    

    current_date = raw_data.select(max(to_date(col("event_time")))).collect()[0][0]

    user_level_df = user_level_df.join(
        user_category_ranked, on="user_id", how="left"
    )

    user_level_df = user_level_df.withColumn(
        "days_since_last_purchase",
        when(
            col("last_purchase_date").isNotNull(),
            datediff(lit(current_date), to_date(col("last_purchase_date")))
        ).otherwise(-1)
    ).drop("last_purchase_date")

    # # Creates the Daily User Activity table
    # daily_user_activity = raw_data.withColumn("session_start_date", to_date(col("session_start_time"))) \
    #                                 .groupBy(col("user_id"), col("session_start_date")) \
    #                                 .agg(
    #                                     count(col("session_id")).alias("num_sessions"),
    #                                     sum(col("session_duration_seconds")).alias("total_duration")
    #                                 ) \
    #                                 .select(
    #                                     "user_id",
    #                                     "session_start_date",
    #                                     "num_sessions",
    #                                     "total_duration"
    #                                 )

    # # Creates the Daily Funnel Analysis table
    # event_schema = ArrayType(StructType([
    #     StructField("timestamp", TimestampType(), True),
    #     StructField("event_type", StringType(), True),
    #     StructField("url", StringType(), True),
    #     StructField("utm_source", StringType(), True)
    # ]))

    # daily_funnel_analysis = raw_data.withColumn("events_list", from_json(col("events_json"), event_schema)) \
    #                                 .withColumn("event", explode(col("events_list"))) \
    #                                 .withColumn("session_start_date", to_date(col("session_start_time"))) \
    #                                 .groupBy(col("session_start_date"), col("event.event_type").alias("step_name")) \
    #                                 .agg(
    #                                     count_distinct(col("user_id")).alias("users_count")
    #                                 ) \
    #                                 .select(
    #                                     "session_start_date",
    #                                     "step_name",
    #                                     "users_count"
    #                                 )

    # # Creates the Daily Traffic Source Report table
    # daily_traffic_source = raw_data.withColumn("session_start_date", to_date(col("session_start_time"))) \
    #                                 .groupBy(col("session_start_date"), col("landing_utm_source")) \
    #                                 .agg(
    #                                     count_distinct(col("user_id")).alias("users_count"),
    #                                     avg(col("session_duration_seconds")).alias("avg_duration")
    #                                 ) \
    #                                 .select(
    #                                     "session_start_date",
    #                                     "landing_utm_source",
    #                                     "users_count",
    #                                     "avg_duration"
    #                                 )
    
    return session_level_df, user_level_df, raw_data

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

    session_level_df, user_level_df, raw_data = create_aggregate_tables(spark, yesterday_date)

    if session_level_df and user_level_df and raw_data:
        # Writes the data to Postgres
        write_to_postgres(session_level_df, "session_level_df")
        write_to_postgres(user_level_df, "user_level_df")
        write_to_postgres(raw_data, "raw_data")

    spark.stop()