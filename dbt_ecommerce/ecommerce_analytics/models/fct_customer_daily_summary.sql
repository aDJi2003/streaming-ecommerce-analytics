WITH base AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    DATE(invoice_timestamp) AS transaction_date,
    customer_id,
    COUNT(DISTINCT invoice_id) AS total_transactions,
    SUM(quantity) AS total_items_purchased,
    SUM(total_price) AS total_revenue

FROM base
GROUP BY 1, 2
ORDER BY transaction_date DESC, customer_id