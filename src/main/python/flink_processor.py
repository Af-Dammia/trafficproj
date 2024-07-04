from kafka import KafkaConsumer
import json
import socket

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'taxi-input'
FLINK_HOST = 'localhost'
FLINK_PORT = 8081
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='taxi-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def send_to_flink(data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((FLINK_HOST, FLINK_PORT))
        s.sendall(json.dumps(data).encode('utf-8'))
        s.sendall(b'\n') 

def main():
    for message in consumer:
        data = message.value
        #print(f"Received data: {data}")
        send_to_flink(data)
        print(f"{data}")

if __name__ == "__main__":
    main()
