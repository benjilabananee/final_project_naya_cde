import sys
sys.path.append('/home/developer/projects/spark-course-python/final_project_naya_cde')
from kafka import KafkaProducer
import time
import json
import requests
import SPARK_MODULE.configuration  as c # type: ignores
from datetime import datetime, timedelta
import base64

base_url = c.stock_data_from_api
end_date = datetime.now() - timedelta(days=1) # Current date

#get_last_cut_data from x_comn
decoded_bytes_last_cut_date = base64.b64decode(sys.argv[1])
decoded_string_last_cut_date = decoded_bytes_last_cut_date.decode('utf-8')
# parts = decoded_string_last_cut_date.split('\n')

start_date = datetime.strptime(decoded_string_last_cut_date.strip(), "%Y-%m-%d") + timedelta(days = 1) #end_date - timedelta(days=1)  # 100 days ago

params = {
    "adjusted": "true",
    "apiKey": c.api_key  
}

REQUESTS_PER_MINUTE = 5 

def fetch_and_produce_stock_data(producer, date: datetime) :
    date_string = date.strftime('%Y-%m-%d')
    url = f"{base_url}{date_string}"
    
    try: 
        response = requests.get(url=url, params=params) 
        response.raise_for_status()  # Raise an HTTPError for bad response s
        parsed_data = response.json()  # Dirctly get the JSON data
        
        for row in parsed_data.get('results', []):
            row['date_time'] = date_string
            producer.send(topic=c.stock_data_topic, value=json.dumps(row).encode('utf-8'))
            print(row)

    except requests.RequestException as e:
        print(f"Failed to retrieve data for {date_string}: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":

    print( '****************************************************'+ str(start_date) + '*****************************************************')
    producer = KafkaProducer(bootstrap_servers=c.kafka_cluster)
    current_date = start_date
    request_count = 0

    while current_date <= end_date:
        if request_count == REQUESTS_PER_MINUTE:
            time.sleep(62)  # Wait for 60 seconds
            request_count = 0

        fetch_and_produce_stock_data(producer, current_date)
        request_count += 1
        current_date += timedelta(days=1)

    producer.flush() 
    producer.close()