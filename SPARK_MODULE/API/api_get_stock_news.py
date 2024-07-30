import requests
import sys
import time
import json
from kafka import KafkaProducer
from datetime import datetime, timedelta
from typing import List

# Ensure you adjust the path to your configuration
sys.path.append('/home/developer/projects/spark-course-python/spark_course_python/final_project_naya_cde/')
import SPARK_MODULE.configuration as c

# Configuration
BASE_URL = f"{c.stock_data_news}{c.api_key}"
MAX_REQUESTS_PER_MINUTE = 5

params = {
    "adjusted": "true",
    "apiKey": c.api_key  # Assuming you have moved the API key to the configuration file
}

def fetch_all_data(base_url: str, max_requests_per_minute: int):
    producer = KafkaProducer(
        bootstrap_servers=c.kafka_cluster, 
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    url = base_url
    request_count = 0

    while url:
        if request_count >= max_requests_per_minute:
            print("Rate limit reached. Waiting for 60 seconds...")
            time.sleep(62)  # wait for 60 seconds
            request_count = 0

        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            break

        data = response.json()
        if 'results' not in data:
            print(f"Error: 'results' key not found in response")
            break

        for item in data.get('results'):
            producer.send(topic=c.stock_news_topic, value=item)

        # Update the URL for the next page and ensure the API key is included
        next_url = data.get('next_url')
        if next_url and 'apiKey=' not in next_url:
            next_url = f"{next_url}&apiKey={c.api_key}"
        url = next_url
        request_count += 1

if __name__ == '__main__':
    fetch_all_data(BASE_URL, MAX_REQUESTS_PER_MINUTE)
