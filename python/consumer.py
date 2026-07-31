import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "practice-group-python",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["practice-topic"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        order = json.loads(msg.value().decode("utf-8"))
        print(f"partition {msg.partition()} offset {msg.offset()} -> {order}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
