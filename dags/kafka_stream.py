import csv
import json 
import time 
import datetime
import requests
import random

from kafka import KafkaProducer
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def get_data():

    # This function fetches a random user from a mock API
    # and returns the first user in the response.
    
    request = requests.get("https://fake-json-api.mock.beeceptor.com/users")
    content = request.json()
    content = content[0]

    return content

def generate_user_event():

    # This function generates a random user event for testing purposes.

    users = [f"user_{i}" for i in range(0, 50)]
    event_type = ["page_view", "click", "add_to_cart", "purchase"]
    pages = ["/home", "/products/1", "/products/2", "/cart", "/checkout"]
    utm_sources = ["facebook", "google", "tiktok_ads", "organic"]

    event = {
        "user_id": random.choice(users),
        "event_type": random.choice(event_type),
        "url": random.choice(pages),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "utm_source": random.choice(utm_sources)
    }

    return event

def json_serializer(data):
    return json.dumps(data).encode('utf-8') 
     
@dag(
    dag_id="Streaming_Kafka_to_Spark",
    schedule="0 1 * * *",
    start_date=datetime.datetime(2025,7,28)
)
    
def run_dag():

    """Streams data from Kafka to Spark using Airflow.
    It generates random user events and sends them to a Kafka topic,    
    then processes the data with a Spark job.
    """
        
    @task
    def kafka_stream(num_records: int):

        file_path = "/opt/spark/RAW_DATA/eCommerce-behavior-data-2019-Oct.csv"
        topic_name = "user-event"

        producer = KafkaProducer(
                                bootstrap_servers = "kafka:29092",
                                value_serializer = json_serializer)
        
        try:
            with open(file_path, mode='r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                print("Start sending data to Kafka...")
                for i, row in enumerate(csv_reader):
                    if i >= num_records:
                        break
                    producer.send(topic_name, row)
                    time.sleep(random.uniform(0.1, 0.5))
                print("Finished sending data.")
        except FileNotFoundError:
            print(f"Error: File not found at {file_path}")
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            producer.close()        
        
        # try:
        #     for i in range(num_records):                
        #         producer.send("user-event", generate_user_event())
        #         time.sleep(random.uniform(0.1, 0.5))
        #     print(f"Successfully sent {num_records} records.")
        # except Exception as e:
        #     print(f"Error sending data: {e}")
        #     raise e
        # finally:
        #     producer.close()

    # Define task
    data_generator_task = kafka_stream(num_records=5000)

    spark_batch_job_task = SparkSubmitOperator(
        task_id="run_spark_batch_job",
        conn_id="spark_default",
        application="/opt/spark/jobs/spark_batch_job.py",
        packages="org.postgresql:postgresql:42.7.7",
        application_args=["{{ ds }}"]
    )

    data_generator_task >> spark_batch_job_task


dags_instance = run_dag()