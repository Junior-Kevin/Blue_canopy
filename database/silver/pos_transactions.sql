USE Blue_canopy;
GO
DROP TABLE IF EXISTS silver.pos_transactions;
GO
WITH base AS (
    SELECT 
        REPLACE([transaction_id], ' ','') transaction_id,
        [transaction_date],
        [store_id],
        [customer_id],
        [cashier_id],
        [payment_method],
        CAST([total_amount] AS FLOAT) AS total_amount
    FROM [Blue_canopy].[bronze].[pos_transactions_raw]
    WHERE [transaction_id] IS NOT NULL
),
cleaned AS (
    SELECT 
        -- Core identifiers
        transaction_id,
        -- Parse transaction_date (remove 'T' and handle timezone)
        CAST(LEFT(transaction_date, 10) AS DATE) AS transaction_date,
        CAST(SUBSTRING(transaction_date, 12, 8) AS TIME) AS transaction_time,
        
        -- Clean store_id
        CASE 
            WHEN store_id LIKE '%-DUP%' THEN LEFT(store_id, CHARINDEX('-DUP', store_id) - 1)
            ELSE store_id
        END AS store_id_clean,
        
        -- Clean customer_id
        CASE 
            WHEN customer_id IS NULL THEN 'WALK-IN-CUSTOMER'
            WHEN customer_id LIKE '%-DUP%' THEN LEFT(customer_id, CHARINDEX('-DUP', customer_id) - 1)
            ELSE customer_id
        END AS customer_id_clean,
        
        -- Clean cashier_id
        CASE 
            WHEN cashier_id LIKE '%-DUP%' THEN LEFT(cashier_id, CHARINDEX('-DUP', cashier_id) - 1)
            ELSE cashier_id
        END AS cashier_id_clean,
        
        -- Clean payment_method
        CASE 
            WHEN payment_method = 'Mixed' THEN 'Mixed (Cash + Card)'
            WHEN payment_method = 'Card' THEN 'Card'
            WHEN payment_method = 'Cash' THEN 'Cash'
            WHEN payment_method = 'Mobile Money' THEN 'Mobile Money'
            WHEN payment_method = 'Cheque' THEN 'Cheque'
            ELSE 'Other'
        END AS payment_method_clean,
        
        -- Transaction amount
        total_amount,
        
        -- Time hierarchies
        YEAR(CAST(LEFT(transaction_date, 10) AS DATE)) AS transaction_year,
        MONTH(CAST(LEFT(transaction_date, 10) AS DATE)) AS transaction_month,
        DATEPART(QUARTER, CAST(LEFT(transaction_date, 10) AS DATE)) AS transaction_quarter,
        FORMAT(CAST(LEFT(transaction_date, 10) AS DATE), 'yyyy-MM') AS transaction_year_month,
        DATEPART(HOUR, CAST(SUBSTRING(transaction_date, 12, 8) AS TIME)) AS transaction_hour,
        DATEPART(WEEKDAY, CAST(LEFT(transaction_date, 10) AS DATE)) AS transaction_weekday_num,
        DATENAME(WEEKDAY, CAST(LEFT(transaction_date, 10) AS DATE)) AS transaction_weekday,
        
        -- Customer type segmentation
        CASE 
            WHEN customer_id IS NULL THEN 'Walk-in (Unknown)'
            WHEN customer_id LIKE '%-DUP%' THEN 'Registered (Needs merge)'
            ELSE 'Registered Customer'
        END AS customer_type,
        
        -- Transaction value tier
        CASE 
            WHEN total_amount >= 1000000 THEN 'High Value (1M+ KES)'
            WHEN total_amount >= 500000 THEN 'Medium-High (500K-1M KES)'
            WHEN total_amount >= 100000 THEN 'Medium (100K-500K KES)'
            WHEN total_amount >= 50000 THEN 'Low-Medium (50K-100K KES)'
            ELSE 'Low Value (<50K KES)'
        END AS transaction_tier,
        
        -- Time of day categorization
        CASE 
            WHEN DATEPART(HOUR, CAST(SUBSTRING(transaction_date, 12, 8) AS TIME)) BETWEEN 6 AND 11 THEN 'Morning (6AM-11AM)'
            WHEN DATEPART(HOUR, CAST(SUBSTRING(transaction_date, 12, 8) AS TIME)) BETWEEN 12 AND 16 THEN 'Afternoon (12PM-4PM)'
            WHEN DATEPART(HOUR, CAST(SUBSTRING(transaction_date, 12, 8) AS TIME)) BETWEEN 17 AND 20 THEN 'Evening (5PM-8PM)'
            ELSE 'Late Night (9PM-5AM)'
        END AS time_of_day,
        
        -- Data quality flags
        CASE 
            WHEN CAST(LEFT(transaction_date, 10) AS DATE) > GETDATE() THEN 'Future date - Invalid'
            WHEN CAST(LEFT(transaction_date, 10) AS DATE) < '2015-01-01' THEN 'Suspicious old date'
            WHEN total_amount <= 0 THEN 'Invalid amount'
            WHEN payment_method NOT IN ('Card', 'Cash', 'Mobile Money', 'Cheque', 'Mixed') THEN 'Invalid payment method'
            ELSE 'Valid'
        END AS quality_flag
        
    FROM base
)

SELECT 
    -- Surrogate key
    ROW_NUMBER() OVER(ORDER BY transaction_id)  AS pos_transaction_key,
    
    -- Identifiers
    transaction_id,
    store_id_clean AS store_id,
    customer_id_clean AS customer_id,
    cashier_id_clean AS cashier_id,
    
    -- Transaction details
    transaction_date,
    transaction_time,
    total_amount,
    payment_method_clean AS payment_method,
    transaction_tier,
    customer_type,
    time_of_day,
    
    -- Time attributes
    transaction_year,
    transaction_month,
    transaction_quarter,
    transaction_year_month,
    transaction_hour,
    transaction_weekday_num,
    transaction_weekday,
    
    -- Quality
    quality_flag,
    
    -- Audit        
    GETDATE() AS etl_load_date,
    'silver.pos_transactions' AS etl_source
    
INTO silver.pos_transactions
FROM cleaned
WHERE quality_flag != 'Future date - Invalid'  -- Filter out future dates
ORDER BY transaction_date DESC, transaction_time DESC;
GO
DROP INDEX IF EXISTS idx_postransactions_transid ON silver.pos_transactions;
GO
CREATE UNIQUE CLUSTERED INDEX idx_postransactions_transid
ON silver.pos_transactions (transaction_id);
