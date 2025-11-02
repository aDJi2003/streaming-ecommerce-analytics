import pandas as pd
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

KAFKA_TOPIC = 'user_events'

print("Membaca dataset Online Retail dari file CSV...")
df = pd.read_csv(
    './OnlineRetail.csv',
    encoding='ISO-8859-1',
    parse_dates=['InvoiceDate']
)

df.dropna(subset=['CustomerID'], inplace=True)
df['CustomerID'] = df['CustomerID'].astype(int)
df['Quantity'] = pd.to_numeric(df['Quantity'])
df['UnitPrice'] = pd.to_numeric(df['UnitPrice'])
df = df[df['Quantity'] > 0]
df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%Y-%m-%d %H:%M:%S')

print("Mengirim data ke Kafka topic:", KAFKA_TOPIC)

for index, row in df.iterrows():
    data = row.to_dict()
    producer.send(KAFKA_TOPIC, value=data)
    print(f"Mengirim data: InvoiceNo {data['InvoiceNo']} - StockCode {data['StockCode']}")
    time.sleep(0.05)

producer.flush()
print("Semua data telah terkirim.")