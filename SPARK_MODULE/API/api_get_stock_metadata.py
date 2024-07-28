import requests
import sys
sys.path.append('/home/developer/projects/spark-course-python/spark_course_python')
from kafka import KafkaProducer
import time
import json
import final_project_naya_cde.SPARK_MODULE.configuration as c
from typing import List

# Configuration
BASE_URL = f"{c.base_url_stock_meta_data}{c.api_key}"
MAX_REQUESTS_PER_MINUTE = 5
KAFKA_TOPIC = "stock_meta_data_to_s3_test"
KAFKA_SERVER = "course-kafka:9092"

def fetch_all_data(base_url: str, max_requests_per_minute: int):
    url = base_url
    all_results = []
    request_count = 0

    while url:
        if request_count >= max_requests_per_minute:
            print("Rate limit reached. Waiting for 60 seconds...")
            time.sleep(62)  # wait for 60 seconds
            request_count = 0
            all_results = []

        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            break

        data = response.json()
        if 'results' not in data:
            print(f"Error: 'results' key not found in response")
            break

        all_results.extend(data['results'])
        send_data_to_kafka(all_results)

        # Update the URL for the next page and ensure the API key is included
        next_url = data.get('next_url')
        if next_url and 'apiKey=' not in next_url:
            next_url = f"{next_url}&apiKey={c.api_key}&request_id={data.get('request_id')}"
        url = next_url
        print(url)

        request_count += 1

def send_data_to_kafka(all_results: List[dict]):
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for row in all_results:
        print(row)
        producer.send(KAFKA_TOPIC, value=row)
    producer.flush()
    producer.close()

if __name__ == '__main__':
    fetch_all_data(BASE_URL, MAX_REQUESTS_PER_MINUTE)