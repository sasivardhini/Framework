import json

from confluent_kafka import Producer
def sink_to_confluent_kafka(data, topic, kafka_config):
    try:
        producer_config = {
                   'bootstrap.servers': kafka_config['bootstrap_servers'],
            'message.max.bytes': kafka_config['max_request_size'],
            'max.request.size': kafka_config['max_request_size'] # 10 MB
        }
        p = Producer(producer_config)
        p.produce(topic, key=str(data['timestamp']), value=json.dumps(data['data']))
        p.flush()
        print(f"Data sent to Kafka topic '{topic}'")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
