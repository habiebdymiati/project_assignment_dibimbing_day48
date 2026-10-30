import argparse
import json
import os
import csv
from confluent_kafka import Consumer, KafkaError
from assignment_config import (
    logger_cons,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
    CONSUMER_GROUP_ID,
    DATA_SINK_FILE
)

session_counts = {}

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, default='default-consumer', help='Nama Consumer')
    return parser.parse_args()

def initialize_csv():
    # Mengambil nama folder dari path file (misal: 'output' dari 'output/assignment_consumer.csv')
    folder_path = os.path.dirname(DATA_SINK_FILE)
    
    # Otomatis membuat folder jika path-nya mengandung folder dan folder tersebut belum ada
    if folder_path and not os.path.exists(folder_path):
        os.makedirs(folder_path, exist_ok=True)
        
    if not os.path.exists(DATA_SINK_FILE):
        try:
            with open(DATA_SINK_FILE, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['partition', 'offset', 'consumer_name', 'user_key', 'action', 'amount', 'event_time'])
        except IOError as e:
            logger_cons.error(f"Failed to initialize CSV: {e}")

def append_to_csv(row_data):
    try:
        with open(DATA_SINK_FILE, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(row_data)
    except IOError as e:
        logger_cons.error(f"Failed to write to CSV: {e}")

def process_message(message, consumer_name):
    try:
        partition = message.partition()
        offset = message.offset()
        
        payload = json.loads(message.value().decode('utf-8'))
        user_key = payload.get('user_key')
        action = payload.get('action')
        amount = payload.get('amount')
        event_time = payload.get('event_time')
        
        if user_key:
            session_counts[user_key] = session_counts.get(user_key, 0) + 1
            
            logger_cons.info(
                f"[{consumer_name}] partition {partition} offset {offset} key={user_key} | "
                f"session counts: {session_counts}"
            )
            
            row_data = [partition, offset, consumer_name, user_key, action, amount, event_time]
            append_to_csv(row_data)
            
    except Exception as e:
        logger_cons.error(f"Error processing message: {e}")

def main():
    args = parse_arguments()
    consumer_name = args.name
    
    # Menginisialisasi CSV beserta foldernya sebelum consumer berjalan
    initialize_csv()
    
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    
    consumer = None
    try:
        consumer = Consumer(conf)
        consumer.subscribe([TOPIC_NAME])
        
        logger_cons.info(f"{consumer_name} joined group {CONSUMER_GROUP_ID}")
        
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger_cons.error(f"Kafka Error: {msg.error()}")
            else:
                process_message(msg, consumer_name)
                
    except KeyboardInterrupt:
        logger_cons.info(f"Stopping Consumer {consumer_name}")
    finally:
        if consumer is not None:
            consumer.close()

if __name__ == '__main__':
    main()
