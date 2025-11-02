-- models/fct_customer_daily_summary.sql

-- ``de-project-kafka`.`ecommerce_analytics`.`stg_transactions`` adalah cara dbt untuk mereferensikan model lain.
-- Ini cara dbt membangun ketergantungan (dependency) antar model.
WITH base AS (
    SELECT * FROM `de-project-kafka`.`ecommerce_analytics`.`stg_transactions`
)

SELECT
    -- Ekstrak tanggal saja dari timestamp
    DATE(invoice_timestamp) AS transaction_date,
    customer_id,

    -- Agregasi metrik bisnis yang berguna
    COUNT(DISTINCT invoice_id) AS total_transactions,
    SUM(quantity) AS total_items_purchased,
    SUM(total_price) AS total_revenue

FROM base
GROUP BY 1, 2 -- Kelompokkan berdasarkan tanggal dan customer_id
ORDER BY transaction_date DESC, customer_id