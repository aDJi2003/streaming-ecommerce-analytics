SELECT
    CAST(InvoiceNo AS STRING) AS invoice_id,
    CAST(StockCode AS STRING) AS stock_code,
    CAST(CustomerID AS INT64) AS customer_id,
    CAST(Country AS STRING) AS country,

    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', InvoiceDate) AS invoice_timestamp,

    CAST(Quantity AS INT64) AS quantity,
    CAST(UnitPrice AS FLOAT64) AS unit_price,
    (CAST(Quantity AS INT64) * CAST(UnitPrice AS FLOAT64)) AS total_price

FROM `de-project-kafka.ecommerce_raw.events`
WHERE
    CustomerID IS NOT NULL AND
    InvoiceNo IS NOT NULL
    