import requests
from kafka import KafkaProducer
import time,json,requests
import  SPARK_MODULE.configuration as c
from typing import List


base_url = c.base_url_stock_meta_data + c.api_keys
max_requests_per_minute = 5



def fetch_all_data():
    url = base_url
    all_results = []
    request_count = 0

    while url:
        if request_count >= max_requests_per_minute:
            print("Rate limit reached. Waiting for 60 seconds...")
            time.sleep(60)  # wait for 60 seconds
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
        
        send_data_to_kaka(all_results) 
        
        # Update the URL for the next page and ensure the API key is included
        next_url = data.get('next_url')
        if next_url:
            if 'apiKey=' not in next_url:
                next_url = f"{next_url}&apiKey={c.api_keys}&request_id={data.get('request_id')}"
        url = next_url
        print(url)

        request_count += 1

    # return all_results


def send_data_to_kaka(all_results: List[str]):
    for row in all_results:
        print(row)
        producer = KafkaProducer(bootstrap_servers="course-kafka:9092")
        producer.send(topic="stock_meta_data_to_s3_test", value=json.dumps(row).encode('utf-8'))
    

if __name__ == '__main__':
    all_data = fetch_all_data()
