
DROP TABLE IF EXISTS silver.ecommerce_orders;
GO
WITH split_cte AS (
    SELECT ROW_NUMBER() OVER(ORDER BY order_id) ecommerce_key,
        order_id,
        CAST(CAST(REPLACE(order_date,'T',' ') AS datetime) AS DATE) order_date,
		FORMAT(CAST(REPLACE(order_date, 'T', ' ') AS DATETIME), 'HH:mm:ss') AS order_time,
        CASE WHEN customer_id LIKE '%DUP' 
		     THEN SUBSTRING(customer_id,1,CHARINDEX('D',customer_id)-2)
			 ELSE customer_id
		END AS customer_id,
        TRIM(REPLACE(delivery_address,'"','')) delivery_address,
        delivery_fee = status,
        value,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY (SELECT NULL)) AS position
    FROM [Blue_canopy].[bronze].[ecommerce_orders_raw]
    CROSS APPLY STRING_SPLIT(
        CASE 
            WHEN total_amount IS NULL OR total_amount = '' 
            THEN 'NULL,NULL,NULL'  -- Placeholder for NULL rows
            ELSE total_amount 
        END, 
        ','
    )
)
SELECT
    order_id,
    order_date,
	order_time,
    customer_id,
    delivery_address,
    delivery_fee,
    MAX(CASE WHEN position = 1 THEN value END) AS payment_method,
    MAX(CASE WHEN position = 2 THEN value END) AS status,
    ABS(CAST(MAX(CASE WHEN position = 3 THEN value END) AS DECIMAL(18,2))) AS amount
INTO silver.ecommerce_orders
FROM split_cte
GROUP BY 
    order_id,
    order_date,
	order_time,
    customer_id,
    delivery_address,
    delivery_fee;



