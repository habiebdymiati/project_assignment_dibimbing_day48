# Hands-On Kafka

# Hands-On 1: Kafka CLI

## 1. Menjalankan Kafka broker lokal dengan Docker Compose

```
docker compose up -d
```

Cek apakah broker sudah berjalan dengan baik:

```
docker compose ps
docker logs kafka --tail 50
```

## 2. Membuat topic bernama `practice-topic` dengan 3 partisi

```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic practice-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

Verifikasi:

```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic practice-topic --bootstrap-server localhost:9092
```

## 3. Membuka sesi producer dan mengirim 5 pesan contoh

```
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic practice-topic --bootstrap-server localhost:9092
```

Perintah ini akan membuka prompt interaktif (`>`). Ketik satu pesan lalu tekan Enter, ulangi sebanyak lima kali, contoh:

```
>hello kafka 1
>hello kafka 2
>hello kafka 3
>hello kafka 4
>hello kafka 5
```

Tekan `Ctrl+C` untuk keluar setelah selesai.

## 4. Membuka sesi consumer dan membaca ulang pesan dari awal

```
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic practice-topic --from-beginning --group practice-group --bootstrap-server localhost:9092
```

Kelima pesan tadi akan tampil (urutan antar-partisi tidak dijamin, hanya urutan di dalam satu partisi yang terjaga). Tekan `Ctrl+C` untuk keluar — ini akan meninggalkan consumer group `practice-group` tetap terdaftar, sehingga langkah 5 punya data untuk dicek.

## 5. Menjalankan `kafka-consumer-groups.sh` untuk mengecek lag consumer

```
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --describe --group practice-group --bootstrap-server localhost:9092
```

Perintah ini menampilkan `CURRENT-OFFSET`, `LOG-END-OFFSET`, dan `LAG` per partisi untuk `practice-group`. Karena consumer sudah membaca semua pesan lalu keluar, lag seharusnya bernilai `0` di setiap partisi.

## 6. Menghapus topic

```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --delete --topic practice-topic --bootstrap-server localhost:9092
```

Verifikasi topic sudah tidak ada di daftar:

```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Membersihkan environment

```
docker compose down -v
```

---

# Hands-On 2: Kafka dengan Python

Broker dan topic yang sama seperti sebelumnya (`practice-topic`) — kali ini dikendalikan dari Python, bukan CLI.

## 1. Install library

Buat dan aktifkan virtual environment (PowerShell):

```
python -m venv kafka_venv
.\kafka_venv\Scripts\Activate.ps1
```

Lalu install client Kafka:

```
pip install confluent-kafka
```

## 2. Konfigurasi broker

Script Python akan mengarah ke broker yang sama (`localhost:9092`) dan topic yang sudah dibuat di Hands-On 1 (`practice-topic`). Pastikan broker masih berjalan:

```
docker compose up -d
```

## 3. Producer script — mengirim 5 event order dalam format JSON

Buat file `producer.py`:

```python
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
```

Script ini sudah tersedia di [python/producer.py](python/producer.py). Jalankan:

```
python python/producer.py
```

## 4. Consumer script — membaca dan menampilkan kembali event tersebut

Buat file `consumer.py`:

```python
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
```

Script ini sudah tersedia di [python/consumer.py](python/consumer.py). Jalankan:

```
python python/consumer.py
```

Tekan `Ctrl+C` untuk keluar setelah kelima event tampil.

## 5. Bonus: bandingkan dengan consumer CLI di Hands-On 1

Jalankan ulang consumer CLI dari Hands-On 1 dengan group baru supaya membaca dari awal:

```
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic practice-topic --from-beginning --bootstrap-server localhost:9092
```

Bandingkan outputnya dengan hasil `consumer.py`.
