
import json
import datetime
from math import radians, sin, cos, sqrt, atan2

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedProcessFunction,MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
import redis


class CalculateSpeedFunction(KeyedProcessFunction):
    def __init__(self):
        super(CalculateSpeedFunction, self).__init__()
        self.last_location_state = ValueStateDescriptor("last_location", Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))
        self.last_timestamp_state = ValueStateDescriptor("last_timestamp", Types.STRING())
        self.speed_sum_state = ValueStateDescriptor("speed_sum", Types.FLOAT())
        self.speed_count_state = ValueStateDescriptor("speed_count", Types.INT())
        self.total_distance_state = ValueStateDescriptor("total_distance", Types.FLOAT())


    def open(self, runtime_context):
        self.last_location = runtime_context.get_state(self.last_location_state)
        self.last_timestamp = runtime_context.get_state(self.last_timestamp_state)
        self.speed_sum = runtime_context.get_state(self.speed_sum_state)
        self.speed_count = runtime_context.get_state(self.speed_count_state)
        self.total_distance = runtime_context.get_state(self.total_distance_state)


    def process_element(self, data, ctx):
        try:
            json_data = json.loads(data)
            latitude = json_data.get('latitude')
            longitude = json_data.get('longitude')
            timestamp_str = json_data.get('date_time')
            taxi_id = json_data.get('taxi_id')

            if not all([latitude, longitude, timestamp_str, taxi_id]):
                raise ValueError("Missing latitude, longitude, timestamp, or taxi_id field")

            current_location = (float(latitude), float(longitude))
            current_timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

            if self.last_location.value() is not None and self.last_timestamp.value() is not None:
                distance = self.haversine(self.last_location.value()[0], self.last_location.value()[1],
                                          current_location[0], current_location[1])
                time_diff = (current_timestamp - datetime.datetime.strptime(self.last_timestamp.value(), '%Y-%m-%d %H:%M:%S')).total_seconds() / 3600.0
                speed = distance / time_diff if time_diff > 0 else 0.0

                # Update speed sum and count
                current_speed_sum = self.speed_sum.value() or 0.0
                current_speed_count = self.speed_count.value() or 0
                new_speed_sum = current_speed_sum + speed
                new_speed_count = current_speed_count + 1

                self.speed_sum.update(new_speed_sum)
                self.speed_count.update(new_speed_count)

                current_total_distance = self.total_distance.value() or 0.0
                new_total_distance = current_total_distance + distance
                self.total_distance.update(new_total_distance)

                self.last_location.update(current_location)
                self.last_timestamp.update(timestamp_str)

                average_speed = new_speed_sum / new_speed_count

                #logger.info(f"Sending to Redis - Taxi ID: {taxi_id}, Average Speed: {average_speed}, Total Distance: {new_total_distance}")

                #redis_client.hset(taxi_id, "average_speed", average_speed)
                #redis_client.hset(taxi_id, "total_distance", new_total_distance)


                result = {
                    'taxi_id': taxi_id,
                    'timestamp': timestamp_str,
                    'speed': speed,
                    'total_distance': new_total_distance,
                    'average_speed': average_speed
                }
                yield json.dumps(result)
                print(json.dumps(result))

            self.last_location.update(current_location)
            self.last_timestamp.update(timestamp_str)

        except ValueError as ve:
            print(f"ValueError processing element: {ve}, data: {data}")
        except Exception as e:
            print(f"Error processing element: {e}, data: {data}")

    @staticmethod
    def haversine(lat1, lon1, lat2, lon2):
        R = 6371.0
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        distance = R * c
        return distance
    
def update_redis(taxi_id, average_speed, total_distance):
    # Initialize Redis connection
    redis_host = 'localhost'
    redis_port = 6379
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    # Update Redis with average speed and total distance
    redis_client.hset(taxi_id, "average_speed", average_speed)
    redis_client.hset(taxi_id, "total_distance", total_distance)


class FormatResultFunction(MapFunction):
    def map(self, value):
        return value

def process_stream(src_topic):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test-consumer-group',
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=src_topic,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    kafka_consumer.set_start_from_earliest()

    ds = env.add_source(kafka_consumer)
    speed_stream = ds.key_by(lambda x: json.loads(x)['taxi_id']) \
                     .process(CalculateSpeedFunction()) \
                     .map(lambda x: json.loads(x) if x is not None else None)  # Convert JSON string to dict for further processing

    speed_stream.print() 

    speed_stream.map(lambda x: (x['taxi_id'], x['average_speed'], x['total_distance'])) \
                .filter(lambda tpl: tpl is not None) \
                .map(lambda tpl: update_redis(*tpl)) \
                .print()
    env.execute('Flink Data Processing Job')


    

if __name__ == "__main__":
    src_topic = "taxi-input"
    process_stream(src_topic)

'''
    speed_stream = ds.key_by(lambda x: json.loads(x)['taxi_id']) \
                     .process(CalculateSpeedFunction()) \
                     .map(FormatResultFunction())

    speed_stream.print() 
'''