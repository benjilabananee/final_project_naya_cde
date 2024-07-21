from kafka import KafkaProducer
import time,json,requests
import configuration as c
from datetime import datetime,timedelta,date

base_url = c.stock_data_from_api
end_date = datetime.now()  - timedelta(days=1) # Current date
start_date = end_date - timedelta(days=5)  # 5 days ago

params = {
    "adjusted": "true",
    "apiKey": "4L8BLqY1mDFQIZnC0OzistwsUfrVHNKT"
}


def fetch_and_produce_stock_data(date):

    date_string = date.strftime('%Y-%m-%d')
    url = f"{base_url}{date_string}"
    response = requests.get(url=url,params=params)

    if response.status_code == 200:

        parsed_data = json.loads(response.text) 
        

        for row in parsed_data['results']: 
                row['date_time'] = date_string
                producer = KafkaProducer(bootstrap_servers="course-kafka:9092")
                producer.send(topic="stock_data_test", value=json.dumps(row).encode('utf-8'))
                print(row)
    else:
        print(f"Failed to retrieve data: {response.status_code}")

if __name__ == "__main__":
    current_date = start_date
    while current_date <= end_date:
        fetch_and_produce_stock_data(current_date)
        current_date += timedelta(days=1)


  
