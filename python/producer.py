import json
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")


orders = [
    {"order_id": 1, "item": "keyboard", "qty": 1, "price": 250000},
    {"order_id": 2, "item": "mouse", "qty": 2, "price": 90000},
    {"order_id": 3, "item": "monitor", "qty": 1, "price": 1500000},
    {"order_id": 4, "item": "headset", "qty": 1, "price": 350000},
    {"order_id": 5, "item": "webcam", "qty": 1, "price": 275000},
]

for order in orders:
    producer.produce(
        "practice-topic",
        value=json.dumps(order).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)

producer.flush()
