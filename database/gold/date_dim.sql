-- Create gold schema
-- Create universal date dimension (critical for all marts)
WITH dates AS (
    SELECT DISTINCT date FROM (
        SELECT start_date AS date FROM silver.campaigns UNION
        SELECT registration_date FROM silver.crm UNION
        SELECT order_date FROM silver.ecommerce_orders UNION
        SELECT date FROM silver.economic UNION
        SELECT feedback_date FROM silver.feedback UNION
        SELECT transaction_date FROM silver.gl_transactions UNION
        SELECT receipt_date FROM silver.goods_receipts UNION
        SELECT movement_date FROM silver.inventory_movements UNION
        SELECT snapshot_date FROM silver.inventory_snapshots UNION
        SELECT transaction_date FROM silver.loyalty_transactions UNION
        SELECT date FROM silver.pos_transactions UNION
        SELECT return_date FROM silver.returns UNION
        SELECT interaction_date FROM silver.service_interactions
    )
)
SELECT 
    date_id = CONVERT(INT, FORMAT(date, 'yyyyMMdd')),
    date,
    year = YEAR(date),
    month = MONTH(date),
    month_name = DATENAME(month, date),
    quarter = DATEPART(quarter, date),
    quarter_name = 'Q' + CAST(DATEPART(quarter, date) AS VARCHAR) + ' ' + CAST(YEAR(date) AS VARCHAR),
    week_number = DATEPART(week, date),
    day_of_month = DAY(date),
    day_of_week = DATEPART(weekday, date),
    day_name = DATENAME(weekday, date),
    is_weekend = CASE WHEN DATEPART(weekday, date) IN (1, 7) THEN 1 ELSE 0 END
FROM dates
ORDER BY date;
