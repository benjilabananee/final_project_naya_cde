from kafka import KafkaProducer
import time
import json
import requests
import configuration as c 
from datetime import datetime, timedelta

base_url = c.stock_data_from_api
end_date = datetime.now() - timedelta(days=1)  # Current date
start_date = end_date - timedelta(days=20)  # 5 days ago

params = {
    "adjusted": "true",
    "apiKey": c.api_key  # Assuming you have moved the API key to the configuration file
}

REQUESTS_PER_MINUTE = 5 

def fetch_and_produce_stock_data(producer, date: datetime) :
    date_string = date.strftime('%Y-%m-%d')
    url = f"{base_url}{date_string}"
    
    try: 
        response = requests.get(url=url, params=params) 
        response.raise_for_status()  # Raise an HTTPError for bad responses
        parsed_data = response.json()  # Directly get the JSON data

        for row in parsed_data.get('results', []):
            row['date_time'] = date_string
            producer.send(topic="stock_data", value=json.dumps(row).encode('utf-8'))
            print(row)
            
    except requests.RequestException as e:
        print(f"Failed to retrieve data for {date_string}: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers="course-kafka:9092")
    current_date = start_date
    request_count = 0

    while current_date <= end_date:
        if request_count == REQUESTS_PER_MINUTE:
            time.sleep(60)  # Wait for 60 seconds
            request_count = 0

        fetch_and_produce_stock_data(producer, current_date)
        request_count += 1
        current_date += timedelta(days=1)

    producer.flush() 
    producer.close()