import requests
import sys
sys.path.append('/home/developer/projects/spark-course-python/spark_course_python/final_project_naya_cde/')
from kafka import KafkaProducer
import time
import json
import SPARK_MODULE.configuration as c
from typing import List

# Configuration
BASE_URL = f"{c.stock_data_news}{c.api_key}"
MAX_REQUESTS_PER_MINUTE = 5

def fetch_all_data(base_url: str, max_requests_per_minute: int):
    url = base_url
    request_count = 0

    while url:
        if request_count >= max_requests_per_minute:
            print("Rate limit reached. Waiting for 60 seconds...")
            time.sleep(62)  # wait for 60 seconds
            request_count = 0

        print(url)

        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            break

        data = response.json()
        print(data.get('results')[1].get('insights')[0]['sentiment'])
        if 'results' not in data:
            print(f"Error: 'results' key not found in response")
            break

        # Update the URL for the next page and ensure the API key is included
        next_url = data.get('next_url')
        if next_url and 'apiKey=' not in next_url:
            next_url = f"{next_url}&apiKey={c.api_key}"
        url = next_url

if __name__ == '__main__':
    fetch_all_data(BASE_URL, MAX_REQUESTS_PER_MINUTE)