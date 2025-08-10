import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

def create_spark():

    # Creates a Spark session with the necessary configurations.

    spark = SparkSession.builder \
                        .appName("SparkPostgres") \
                        .config("spark.jars.packages",
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                                "org.postgresql:postgresql:42.6.0") \
                        .config("spark.cores.max", "1") \
                        .config("spark.executor.memory", "512m") \
                        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def read_kafka(spark):

    # Reads data from a Kafka topic and returns a DataFrame.

    df_kafka = spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:29092") \
                    .option("subscribe", "user-event") \
                    .option("startingOffsets", "earliest") \
                    .option("failOnDataLoss", "false") \
                    .load()
    
    return df_kafka

def format_kafka_df(kafka_df):

    # Formats the Kafka DataFrame by extracting the JSON data and converting it to a structured format.

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("url", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("utm_source", StringType(), True)
    ])

    spark_df = kafka_df.selectExpr("CAST(value as STRING)") \
                        .select(from_json("value", schema=schema).alias("data")) \
                        .select("data.*")
    
    return spark_df

def transform_data(spark_df):

    # Transforms the Spark DataFrame by creating user sessions.

    session_df = spark_df.withWatermark("timestamp", "10 minutes") \
                        .groupBy(
                            col("user_id"),
                            window(col("timestamp"), "1 minutes")
                        ).agg(
                            collect_list(
                                struct(col("timestamp"), col("event_type"), col("url"), col("utm_source"))
                            ).alias("events")
                        )

    session_df_transfrom = session_df.withColumn("session_id", expr("uuid()")) \
                                    .withColumn("session_start_time", element_at(col("events"), 1).getField("timestamp")) \
                                    .withColumn("session_end_time", element_at(col("events"), size(col("events"))).getField("timestamp")) \
                                    .withColumn("session_duration_seconds", col("session_end_time").cast("long") - col("session_start_time").cast("long")) \
                                    .withColumn("session_duration_minutes", round(col("session_duration_seconds") / 60, 2)) \
                                    .withColumn("number_of_events", size(col("events"))) \
                                    .withColumn("landing_page", element_at(col("events"), 1).getField("url")) \
                                    .withColumn("exit_page", element_at(col("events"), size(col("events"))).getField("url")) \
                                    .withColumn("landing_utm_source", element_at(col("events"), 1).getField("utm_source")) \
                                    .withColumn("events_json", to_json(col("events"))) \
                                    .withColumn("date", current_date())

    session_df_transfrom = session_df_transfrom.select(
        "session_id",
        "user_id",
        "session_start_time",
        "session_end_time",
        "session_duration_seconds", 
        "session_duration_minutes",
        "number_of_events",
        "landing_page",
        "exit_page",
        "landing_utm_source",
        "events_json",
        "date"
    )
    
    return session_df_transfrom

def create_postgres_connection():

    # Creates a connection to the PostgreSQL database.

    conn = psycopg2.connect(
        dbname = "airflow",
        user = "airflow",
        password = "airflow",
        host = "postgres",
        port = "5432"
    )

    return conn

def create_postgres_table(cursor, drop=True):

    # Creates a PostgreSQL table for storing user session data.

    if drop:
        cursor.execute("DROP TABLE IF EXISTS user_session;")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS user_session (
            session_id TEXT,
            user_id TEXT,
            session_start_time TIMESTAMPTZ,
            session_end_time TIMESTAMPTZ,
            session_duration_seconds BIGINT, 
            session_duration_minutes NUMERIC(10, 2),
            number_of_events INT,
            landing_page TEXT,
            exit_page TEXT,
            landing_utm_source TEXT,
            events_json TEXT,
            date TIMESTAMPTZ
        );
    """
    )
    
def write_to_postgres(batch_df, batch_id):

    # Writes the processed batch DataFrame to PostgreSQL.

    print(f"--- Processing batch {batch_id} with {batch_df.count()} rows ---")

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/airflow") \
        .option("dbtable", "user_session") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"--- Batch {batch_id} written to PostgreSQL ---")

if __name__ == "__main__":

    spark = create_spark()
    postgres_conn = create_postgres_connection()

    # Connect to Postgres and create table
    postgres_cursor = postgres_conn.cursor()
    create_postgres_table(postgres_cursor, drop=True)
    postgres_conn.commit()
    postgres_cursor.close()
    postgres_conn.close()

    # Read from Kafka and process data
    kafka_df = read_kafka(spark)
    spark_df = format_kafka_df(kafka_df=kafka_df)
    spark_df_transform = transform_data(spark_df)

    query = spark_df_transform.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", "file:///opt/spark/data_lake/sessions") \
                .option("checkpointLocation", "file:///opt/spark/data_lake/checkpoints/kafka_to_datalake") \
                .partitionBy("date") \
                .start()
    
    query.awaitTermination()