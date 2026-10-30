import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
from assignment_config import (
    logger_prod,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
    PRODUCER_INTERVAL_SECONDS,
    USERS_LIST
)

def delivery_report(err, msg):
    """ Callback log pengiriman pesan sesuai format screenshot """
    if err is not None:
        logger_prod.error(f"Message delivery failed: {err}")
    else:
        key_decoded = msg.key().decode('utf-8') if msg.key() else 'None'
        logger_prod.info(f"Delivered key={key_decoded} -> partition {msg.partition()} offset {msg.offset()}")

def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    
    try:
        producer = Producer(conf)
        logger_prod.info(f"Created topic {TOPIC_NAME}")
        logger_prod.info(f"Producing to {TOPIC_NAME} every 5s")
        
        while True:
            user_key = random.choice(USERS_LIST)
            
            payload = {
                'user_key': user_key,
                'action': random.choice(['click', 'view', 'purchase', 'logout']),
                'amount': round(random.uniform(10.0, 500.0), 2),
                'event_time': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+00:00'
            }
            
            json_payload = json.dumps(payload)
            
            producer.produce(
                topic=TOPIC_NAME,
                key=user_key,
                value=json_payload,
                callback=delivery_report
            )
            
            producer.poll(0)
            time.sleep(5)
            
    except KeyboardInterrupt:
        logger_prod.info("Stopping producer")
    except Exception as e:
        logger_prod.error(f"An error occurred: {e}")
    finally:
        producer.flush(timeout=5)

if __name__ == '__main__':
    main()