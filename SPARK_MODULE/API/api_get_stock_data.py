from kafka import KafkaProducer
import time,json,requests
import configuration as c
from datetime import datetime

url = c.stock_data_from_api

params = {
    "adjusted": "true",
    "apiKey": "4L8BLqY1mDFQIZnC0OzistwsUfrVHNKT"
}


def fetch_and_produce_stock_data():
    response = requests.get(url=url,params=params)
    
    if response.status_code == 200:

        parsed_data = json.loads(response.text) 
        date_string = '2023-01-09'

        for row in parsed_data['results']:   
                row['date_time'] = str(datetime.strptime(date_string, "%Y-%m-%d").date())
                producer = KafkaProducer(bootstrap_servers="course-kafka:9092")
                producer.send(topic="stock_data_test", value=json.dumps(row).encode('utf-8'))
                print(row)
    else:
        print(f"Failed to retrieve data: {response.status_code}")

if __name__ == "__main__":
    fetch_and_produce_stock_data()


  
