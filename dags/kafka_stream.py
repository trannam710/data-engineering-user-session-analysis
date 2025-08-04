import json 
import time 
import datetime
import requests

from kafka import KafkaProducer
from airflow.decorators import dag, task

def get_data():
    
    request = requests.get("https://fake-json-api.mock.beeceptor.com/users")
    content = request.json()
    content = content[0]

    return content

def json_serializer(data):
    return json.dumps(data).encode('utf-8') 
     
@dag(
    dag_id="My-First-DAG",
    schedule="@daily",
    start_date=datetime.datetime(2025,7,28)
)
    
def run_dag():
        
    @task
    def kafa_stream():

        producer = KafkaProducer(
                                bootstrap_servers = "kafka:29092",
                                value_serializer = json_serializer)
        
        start_time = time.time()
        while True:
            if time.time() - start_time > 60:
                break
            
            producer.send("user-topic", get_data())
            time.sleep(5)

    # Define task
    kafa_stream()

dags_instance = run_dag()