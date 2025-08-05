import json 
import time 
import datetime
import requests
import random

from kafka import KafkaProducer
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def get_data():
    
    request = requests.get("https://fake-json-api.mock.beeceptor.com/users")
    content = request.json()
    content = content[0]

    return content

def generate_user_event():

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
    dag_id="My-First-DAG",
    schedule="0 1 * * *",
    start_date=datetime.datetime(2025,7,28)
)
    
def run_dag():
        
    @task
    def kafka_stream(num_records: int):

        producer = KafkaProducer(
                                bootstrap_servers = "kafka:29092",
                                value_serializer = json_serializer)
        
        
        try:
            for i in range(num_records):                
                producer.send("user-event", generate_user_event())
                time.sleep(random.uniform(0.1, 0.5))
            print(f"Successfully sent {num_records} records.")
        except Exception as e:
            print(f"Error sending data: {e}")
            raise # Báo lỗi để Airflow biết tác vụ thất bại
        finally:
            producer.close()

    # Define task
    data_generator_task = kafka_stream(num_records=5000)

    spark_batch_job_task = SparkSubmitOperator(
        task_id="run_spark_batch_job",
        conn_id="spark_default",
        application="/opt/spark/jobs/spark_batch_job.py",
        application_args=["{{ ds }}"]
    )

    data_generator_task >> spark_batch_job_task


dags_instance = run_dag()