import json
from kafka import KafkaConsumer
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./de-project-kafka.json"

TABLE_ID = "de-project-kafka.ecommerce_raw.events"

try:
    client = bigquery.Client()
    print("Berhasil terhubung ke BigQuery.")
except Exception as e:
    print(f"Gagal terhubung ke BigQuery: {e}")
    exit()

consumer = KafkaConsumer(
    'user_events',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='bigquery-loader-group-1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer siap menerima data dari Kafka...")

for message in consumer:
    event_data = message.value
    rows_to_insert = [event_data]
    
    try:
        errors = client.insert_rows_json(TABLE_ID, rows_to_insert)
        
        if not errors:
            print(f"Data berhasil dimasukkan: InvoiceNo {event_data.get('InvoiceNo')}")
        else:
            print(f"Gagal memasukkan data ke BigQuery: {errors}")
            
    except Exception as e:
        print(f"Terjadi error saat mengirim ke BigQuery: {e}")