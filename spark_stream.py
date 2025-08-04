import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, from_json
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

def create_spark():

    spark = SparkSession.builder \
                        .appName("SparkPostgres") \
                        .config("spark.jars.packages",
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                                "org.postgresql:postgresql:42.6.0") \
                        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def read_kafka(spark):

    df_kafka = spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("subscribe", "user-topic") \
                    .option("startingOffsets", "latest") \
                    .load()
    
    return df_kafka

def format_kafka_df(kafka_df):

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("company", StringType(), False),
        StructField("username", StringType(), False),
        StructField("email", StringType(), False),
        StructField("address", StringType(), False),
        StructField("zip", StringType(), False),
        StructField("state", StringType(), False),
        StructField("country", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("photo", StringType(), False)
    ])

    spark_df = kafka_df.selectExpr("CAST(value as STRING)") \
                        .select(from_json("value", schema=schema).alias("data")) \
                        .select("data.*")
    
    return spark_df

def create_postgres_connection():

    conn = psycopg2.connect(
        dbname = "airflow",
        user = "airflow",
        password = "airflow",
        host = "localhost",
        port = "5432"
    )

    return conn

def create_postgres_table(cursor):

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_test (
            id TEXT,
            name TEXT,
            company TEXT,
            username TEXT,
            email TEXT,
            address TEXT,
            zip TEXT,
            state TEXT,
            country TEXT,
            phone TEXT,
            photo TEXT
        );
    """
    )
    
def write_to_postgres(batch_df, batch_id):

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/airflow") \
        .option("dbtable", "spark_test") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

if __name__ == "__main__":

    spark = create_spark()
    postgres_conn = create_postgres_connection()

    # Connect to Postgres and create table
    postgres_cursor = postgres_conn.cursor()
    create_postgres_table(postgres_cursor)
    postgres_conn.commit()
    postgres_cursor.close()
    postgres_conn.close()

    kafka_df = read_kafka(spark)

    spark_df = format_kafka_df(kafka_df=kafka_df)

    query = spark_df.writeStream \
                .outputMode("append") \
                .foreachBatch(write_to_postgres) \
                .option("checkpointLocation", "/tmp/spark_checkpoint/kafka_pg") \
                .start()
    
    query.awaitTermination()