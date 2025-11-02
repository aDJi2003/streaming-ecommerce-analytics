-- models/stg_transactions.sql

SELECT
    -- Mengubah tipe data dan nama kolom agar lebih jelas
    CAST(InvoiceNo AS STRING) AS invoice_id,
    CAST(StockCode AS STRING) AS stock_code,
    CAST(CustomerID AS INT64) AS customer_id,
    CAST(Country AS STRING) AS country,

    -- Mengubah string tanggal menjadi tipe data TIMESTAMP yang sebenarnya
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', InvoiceDate) AS invoice_timestamp,

    -- Menghitung total harga untuk setiap baris produk
    CAST(Quantity AS INT64) AS quantity,
    CAST(UnitPrice AS FLOAT64) AS unit_price,
    (CAST(Quantity AS INT64) * CAST(UnitPrice AS FLOAT64)) AS total_price

FROM `de-project-kafka.ecommerce_raw.events` -- ✅ Sumber data mentah kita
WHERE
    CustomerID IS NOT NULL AND
    InvoiceNo IS NOT NULL