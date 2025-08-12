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
        StructField("product_id", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_session", StringType(), True)
    ])

    spark_df = kafka_df.selectExpr("CAST(value as STRING)") \
                        .select(from_json("value", schema=schema).alias("data")) \
                        .select("data.*")
    
    return spark_df

def transform_data(spark_df):

    # Transforms the Spark DataFrame by creating user sessions.
    
    session_df_transfrom = spark_df.withColumn("extracted_date", current_date()) \
                                                .withColumn("date", to_date(col("event_time"))) \
                                                .withColumn("date_of_week", date_format(col("event_time"), "EEEE")) \
                                                .withColumn("hour_of_day", hour(col("event_time"))) \
                                                .withColumn("main_category", when(col("category_code").isNotNull(), split(col("category_code"), ".").getItem(0)).otherwise("unknown")) \
                                                .withColumn("sub_category", when(col("category_code").isNotNull(), split(col("category_code"), ".").getItem(1)).otherwise("unknown")) \

    session_df_transfrom = session_df_transfrom.select(
        "user_session",
        "user_id",
        "product_id",
        "event_time",
        "event_type",
        "category_id",
        "category_code",
        "brand",
        "price",
        "extracted_date",
        "date",
        "date_of_week",
        "hour_of_day",
        "main_category",
        "sub_category"
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
            user_session TEXT PRIMARY KEY,
            user_id TEXT,
            session_id TEXT,
            session_start_time TIMESTAMPTZ,
            session_end_time TIMESTAMPTZ,
            session_duration_seconds BIGINT, 
            session_duration_minutes NUMERIC(10, 2),
            number_of_events INT,
            first_product_id TEXT,
            last_product_id TEXT,
            first_product_category TEXT,
            last_product_category TEXT,
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
                .partitionBy("extracted_date") \
                .start()
    
    query.awaitTermination()