from kafka import KafkaProducer
import time,json,requests
import  SPARK_MODULE.configuration as c

url = c.stock_data_from_api

params = {
    "adjusted": "true",
    "apiKey": "4L8BLqY1mDFQIZnC0OzistwsUfrVHNKT"
}

response = requests.get(url=url,params=params)

def fetch_and_produce_stock_data():
    if response.status_code == 200:

        parsed_data = json.loads(response.text) 

        for row in parsed_data['results']:
                print(row)
                producer = KafkaProducer(bootstrap_servers="course-kafka:9092")
                producer.send(topic="stock_data", value=json.dumps(row).encode('utf-8'))
                time.sleep(1)
    else:
        print(f"Failed to retrieve data: {response.status_code}")

if __name__ == "__main__":
    fetch_and_produce_stock_data()


  
