import os
from datetime import datetime, timezone
import yaml
from source.binance import fetch_binance_data
from source.currencies import fetch_currency_data
from source.cryptocurrency import fetch_cryptocurrency_data
from sink.local_storage import sink_to_json
from sink.sql import insert_data, sink_to_mysql
from sink.confluent_kafka import sink_to_confluent_kafka

def load_config(config_file):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def transform_data(data, add_timestamp=True):
    if add_timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()
        return {"timestamp": timestamp, "data": data}
    return {"data": data}

def process_data(source_name, fetch_function, config):
    data = fetch_function()
    if data:
        transformed_data = transform_data(data, config['transformer']['add_timestamp'])
        for sink in config['sinks']:
            if sink['type'] == 'json':
                file_path = sink.get('file_path', {}).get(source_name)
                if file_path:
                    sink_to_json(transformed_data, file_path)
                else:
                    print(f"No file path configured for {source_name} in JSON sink.")
            elif sink['type'] == 'mysql':
                db_config = sink.get('mysql', {})
                table_name = db_config.get('table_name', {}).get(source_name)
                if table_name:
                    sink_to_mysql(transformed_data, db_config, table_name)
                else:
                    print(f"No table name configured for {source_name} in MySQL sink.")
            elif sink['type'] == 'kafka':
                kafka_config = sink.get('kafka', {})
                topic = kafka_config.get('topic', {}).get(source_name)
                if topic:
                    sink_to_confluent_kafka(transformed_data, topic, kafka_config)
                else:
                    print(f"No topic configured for {source_name} in Kafka sink.")
            else:
                print(f"Unsupported sink type: {sink['type']}")

def main():
    config_path = './config/config.yaml'
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        return

    config = load_config(config_path)

    print("Select a data source:")
    print("- 1: Binance")
    print("- 2: Cryptocurrency Market")
    print("- 3: Currencies")

    selected_source = input("Enter your choice (1/2/3): ")

    if selected_source == '1':
        fetch_function = fetch_binance_data
        source_name = 'binance'
    elif selected_source == '2':
        fetch_function = fetch_cryptocurrency_data
        source_name = 'cryptocurrency'
    elif selected_source == '3':
        fetch_function = fetch_currency_data
        source_name = 'currency'
    else:
        print("Invalid source selection.")
        return

    data = fetch_function()
    transformed_data = transform_data(data, config['transformer']['add_timestamp'])

    print("Select a sink:")
    print("- 1: JSON")
    print("- 2: MySQL")
    print("- 3: Kafka")

    selected_sink = input("Enter your choice (1/2/3): ")

    if selected_sink == '1':
        file_path = config['sinks'][0].get('file_path', {}).get(source_name)
        if file_path:
            sink_to_json(transformed_data, file_path)
            print(f"Data stored as JSON at {file_path}")
        else:
            print(f"No file path configured for {source_name} in JSON sink.")

    elif selected_sink == '2':
        mysql_config = config['sinks'][1].get('mysql', {})
        table_name = mysql_config.get('table_name', {}).get(source_name)
        if table_name:
            sink_to_mysql(transformed_data, mysql_config, table_name)
            print(f"Data inserted into MySQL table {table_name}")
        else:
            print(f"No table name configured for {source_name} in MySQL sink.")

    elif selected_sink == '3':
        kafka_config = config['sinks'][2].get('kafka', {})
        topic = kafka_config.get('topic', {}).get(source_name)
        if topic:
            sink_to_confluent_kafka(transformed_data, topic, kafka_config)
            print(f"Data sent to Kafka topic {topic}")
        else:
            print(f"No topic configured for {source_name} in Kafka sink.")
    else:
        print("Invalid sink selection.")

if __name__ == "__main__":
    main()
