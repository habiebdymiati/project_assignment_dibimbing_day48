# Assignment Day 48


## 1. Menjalankan kafka melalui docker

jalan perintah container kafka menggunakan docker dengan mengetikan perintah
```
docker compose up -d
``` 

## 2. Buat Virtual Environment 

Buat dan aktifkan virtual environment (PowerShell):

```
python -m venv kafka_venv
.\kafka_venv\Scripts\Activate.ps1
```

Lalu install client Kafka dan Faker:

```
pip install confluent-kafka
Pip install faker
```

## 2. Jalankan Producer

Jalankan file assingment_producer.py untuk menggenerate random event dari faker yang nantinya akan di proses kafka

```
python python/assignment_producer.py
```
event berisi partition,offset,consumer_name,user_key,action,amount,event_time

## 3. Jalankan Consumer

Buka dua terminal, tanpa menutup terminal yang menjalankan file assignment_producer.py
Jalankan file assingment_consumer.py di dua terminal berbeda dengan mengetikan perintah

```
python python/assignment_consumer.py --name consumer-1
```
di terimal satu dan 

```
python python/assignment_consumer.py --name consumer-2
```
di terimal lainnya.

## 4. Pantau Hasil Output

Pantau hasil output dari pemrosesan yang dilakukan consumer di file output.CSV di folder output,
jika ingin menghentikan producer atau consumer, Tekan `Ctrl+C` untuk keluar di ketiga terimal tadi


## Membersihkan environment

Hentikan dan hapus container beserta volume data Kafka:

```
docker compose down -v
```

# Laporan Hasil Analisis & Observasi Sistem Data Streaming Apache Kafka

**Proyek:** Kafka Hands-on Basic  
**Komponen:** Producer, Multi-Consumer Group, dan Data Sink (CSV)

---

## A. Penjelasan Mekanisme Penentuan Alokasi Partisi Berdasarkan Key (Key-Based Partitioning)

Pada berkas `assignment_producer.py`, pengiriman pesan dilakukan menggunakan fungsi berikut:
```python
producer.produce(
    topic=TOPIC_NAME,
    key=user_key,
    value=json_payload,
    callback=delivery_report
)
```

Meskipun kita tidak menentukan jenis partitioner secara eksplisit di dalam konfigurasi variabel `conf`, Apache Kafka melalui library `confluent-kafka` (`librdkafka`) menerapkan strategi **Default Key-Based Partitioning** bawaan menggunakan algoritma **MurmurHash2**.

### 1. Bagaimana Kafka Menentukan ke Partition Mana Sebuah Event Dikirim?



Ketika sebuah event dikirim menggunakan kata kunci (`key`), Kafka menentukan nomor partisi tujuan secara otomatis melalui **3 langkah sederhana**:

1. **Membaca Key:** Kafka melihat `user_key` dari data yang dikirim (misalnya: `"user_1"`, `"user_2"`, atau `"user_3"`).
2. **Mengubah Jadi Angka Unik (Hashing):** Kafka memasukkan teks kunci tersebut ke dalam mesin pengacak bawaan bernama **MurmurHash2**. Mesin ini bertugas mengubah teks string menjadi sebuah angka unik yang acak namun selalu konsisten.
3. **Pembagian Sisa (Modulo):** Angka unik tersebut dibagi dengan total jumlah partisi yang tersedia pada topik (pada proyek ini ada **3 partisi**: `0, 1, 2`). Sisa dari hasil pembagian tersebut dijadikan sebagai nomor partisi tujuan.

**Contoh Sederhananya:**
* Kunci `"user_1"` setelah dihitung menghasilkan sisa pembagian **2** $\rightarrow$ Selalu dikirim ke **Partisi 2**.
* Kunci `"user_2"` setelah dihitung menghasilkan sisa pembagian **0** $\rightarrow$ Selalu dikirim ke **Partisi 0**.
* Kunci `"user_3"` setelah dihitung menghasilkan sisa pembagian **1** $\rightarrow$ Selalu dikirim ke **Partisi 1**.

**Keuntungan Cara Ini:**
* **Data Tidak Tertukar:** Semua transaksi milik `user_1` dijamin akan selalu masuk ke partisi yang sama secara berurutan, sehingga riwayat waktunya tidak akan berantakan.
* **Beban Adil:** Karena pengguna dipilih secara acak, data otomatis tersebar merata ke partisi `0`, `1`, dan `2` tanpa ada partisi yang menganggur.
---

## B. Analisis Kelompok Konsumen (Consumer Group Insight)

Ketika kita menjalankan minimal dua consumer ke dalam kelompok yang sama (`group.id = 'events_group'`), Kafka mengaktifkan fitur **Group Coordinator** untuk membagi beban pembacaan data secara paralel dan efisien.

Berikut adalah hasil observasi dari pembagian kerja tersebut:

### 1. Hasil Observasi Log Terminal
Saat program dijalankan bersamaan menggunakan parameter `--name consumer-1` dan `--name consumer-2`, Kafka mendistribusikan pengelolaan 3 partisi (`0, 1, 2`) secara otomatis ke dalam kelompok:

* **Terminal `consumer-1`:**
  ```text
  2026-10-30 14:05:01 | INFO | consumer | consumer-1 joined group events_group
  2026-10-30 14:05:01 | INFO | consumer | Assigned partitions:
  2026-10-30 14:05:05 | INFO | consumer | [consumer-1] partition 0 offset 61 key=user_2 | session counts: {'user_2': 1}
  2026-10-30 14:05:15 | INFO | consumer | [consumer-1] partition 1 offset 34 key=user_3 | session counts: {'user_2': 1, 'user_3': 1}
  ```
* **Terminal `consumer-2`:**
  ```text
  2026-10-30 14:05:02 | INFO | consumer | consumer-2 joined group events_group
  2026-10-30 14:05:02 | INFO | consumer | Assigned partitions:
  2026-10-30 14:05:10 | INFO | consumer | [consumer-2] partition 2 offset 42 key=user_1 | session counts: {'user_1': 1}
  ```

### 2. Mekanisme Pembagian Data & Kerja Kelompok
Berdasarkan log yang teramati, perilaku pembagian data terbukti mengikuti aturan baku arsitektur Kafka:

* **Prinsip Eksklusivitas Partisi:** Satu partisi hanya boleh dibaca oleh maksimal satu consumer di dalam group yang sama. Pada log di atas, Partisi `0` dan `1` dikelola eksklusif oleh `consumer-1`, sedangkan Partisi `2` dikelola oleh `consumer-2`. Hal ini menjamin tidak ada data yang diproses ganda (*double processing*).
* **Paralelisme Seimbang:** Distribusi beban terbagi adil. `consumer-1` memproses pesan-pesan milik `user_2` dan `user_3` (karena hash key mereka jatuh di partisi 0 dan 1). Sementara `consumer-2` fokus menangani data transaksi milik `user_1` (partisi 2).
* **Efek Pergerakan Nilai Offset:** Karena data dikonsumsi dari beberapa partisi yang berbeda secara paralel, urutan baris log pencatatan data dan pergerakan angka *offset* yang tercetak bergerak melompat-lompat secara independen sesuai waktu kedatangan pesan di masing-masing partisi internal broker, bukan berurutan linear tunggal dari 1 sampai akhir.
* **Isolasi State Session Counts:** Penghitungan jumlah event (`session_counts`) terisolasi dengan aman di memori lokal masing-masing consumer. `consumer-2` hanya melacak total data milik `user_1`, sementara data akumulasi `user_2` dan `user_3` terkelola penuh di dalam memori `consumer-1`.

### 3. Representasi Hasil Akhir pada Berkas `events.csv`
Data dari kedua konsumen tersebut berhasil ditulis (append) secara aman ke dalam berkas sink tunggal yang sama dengan struktur baris records sebagai berikut:

| partition | offset | consumer_name | user_key | action | amount | event_time |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 0 | 61 | consumer-1 | user_2 | view | 250.50 | 2026-10-30T07:05:05.123+00:00 |
| 2 | 42 | consumer-2 | user_1 | purchase | 499.99 | 2026-10-30T07:05:10.456+00:00 |
| 1 | 34 | consumer-1 | user_3 | click | 15.20 | 2026-10-30T07:05:15.789+00:00 |

### Kesimpulan Observasi:
Uji coba membuktikan bahwa arsitektur klaster Kafka dengan jumlah partisi 