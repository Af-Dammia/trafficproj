import os
import time
from json import dumps, loads
from kafka import KafkaProducer

kafka_nodes = "localhost:9092"
myTopic = "taxi-input"
data_folder = "/Users/imtiyazansari/projtraffic/release/taxi_log_2008_by_id"

def gen_data():
    prod = KafkaProducer(bootstrap_servers=kafka_nodes, value_serializer=lambda x: dumps(x).encode('utf-8'))
    
    for filename in os.listdir(data_folder):
        if filename.endswith(".txt"):  
            file_path = os.path.join(data_folder, filename)
            print(f"Processing file: {file_path}")
            
            with open(file_path, 'r') as file:
                for line in file:
                    try:
                        taxi_id, date_time, longitude, latitude = line.strip().split(',')
                        taxi_data = {
                            'taxi_id': taxi_id,
                            'date_time': date_time,
                            'longitude': float(longitude),
                            'latitude': float(latitude)
                        }
                        print(f"Sending data: {taxi_data}")
                        prod.send(topic=myTopic, value=taxi_data)
                        #time.sleep(1)  # Sleep for a second to simulate real-time data flow
                    except ValueError as e:
                        print(f"Skipping line due to parsing error: {line} | Error: {e}")
    
    prod.flush()
    print("All data sent successfully!")


if __name__ == "__main__":
    gen_data()