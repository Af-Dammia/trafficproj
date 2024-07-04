
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer,FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema


def process_stream(kafka_broker, src_topic, target_topic):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///Users/imtiyazansari/projtraffic/src/main/python/flink-connector-kafka-1.17.2.jar", \
        "file:///Users/imtiyazansari/projtraffic/src/main/python/kafka-clients-2.8.1.jar", \
        "file:///Users/imtiyazansari/projtraffic/src/main/python/hadoop-common-2.7.7.jar"
    )

    kafka_props = {
        'bootstrap.servers': kafka_broker,
        'group.id': 'test-consumer-group',
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=src_topic,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    kafka_consumer.set_start_from_earliest()
    
    kafka_producer = FlinkKafkaProducer(
        topic=target_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_broker}
    )

    ds = env.add_source(kafka_consumer)
    ds.add_sink(kafka_producer)

    env.execute('Kafka to Kafka Flink Job')



if __name__ == "__main__":
    kafka_broker = "localhost:9092"
    src_topic = "taxi-input"
    target_topic = "taxi-output"
    process_stream(kafka_broker, src_topic, target_topic)
