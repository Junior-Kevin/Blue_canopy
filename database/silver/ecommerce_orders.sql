WITH base AS (
    SELECT 
        order_id,
        order_date,
        customer_id,
        delivery_address,
        delivery_fee,
        payment_method,
        status,
        total_amount
    FROM [Blue_canopy].[bronze].[ecommerce_orders_raw]
),

parsed AS (
    SELECT 
        -- Order ID
        order_id,
        
        -- Parse order_date (remove 'T' and timezone)
        CAST(LEFT(order_date, 10) AS DATE) AS order_date,
        CAST(SUBSTRING(order_date, 12, 8) AS TIME) AS order_time,
        
        -- Clean customer_id (remove -DUP suffix)
        CASE 
            WHEN customer_id LIKE '%-DUP%' THEN LEFT(customer_id, CHARINDEX('-DUP', customer_id) - 1)
            ELSE customer_id
        END AS customer_id_clean,
        
        -- Clean delivery_address (remove quotes)
        REPLACE(REPLACE(delivery_address, '"', ''), '|', ',') AS delivery_address_clean,
        
        -- Delivery fee
        TRY_CAST(delivery_fee AS FLOAT) AS delivery_fee_kes,
        
        -- Parse payment_method (contains: payment,status,amount)
        -- Using PARSENAME (reads right to left: position 1 is rightmost)
        LTRIM(RTRIM(PARSENAME(REPLACE(payment_method, ',', '.'), 3))) AS payment_method_clean,
        LTRIM(RTRIM(PARSENAME(REPLACE(payment_method, ',', '.'), 2))) AS status_clean,
        TRY_CAST(LTRIM(RTRIM(PARSENAME(REPLACE(payment_method, ',', '.'), 1))) AS FLOAT) AS total_amount_clean
        
    FROM base
)

SELECT 
    -- Surrogate key
    order_id AS order_key,
    
    -- Order identifiers
    order_id,
    
    -- Customer
    customer_id_clean AS customer_id,
    
    -- Dates
    order_date,
    order_time,
    
    -- Address
    delivery_address_clean AS delivery_address,
    
    -- Financial
    delivery_fee_kes,
    total_amount_clean,
    total_amount_clean - delivery_fee_kes AS subtotal_kes,
    
    -- Payment & Status
    payment_method_clean AS payment_method,
    status_clean AS order_status,
    
    -- Time attributes (for analysis)
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    DATEPART(QUARTER, order_date) AS order_quarter,
    FORMAT(order_date, 'yyyy-MM') AS order_year_month,
    DATENAME(WEEKDAY, order_date) AS order_weekday,
    DATEPART(HOUR, order_time) AS order_hour,
    
    -- Data quality
    CASE 
        WHEN total_amount_clean IS NULL OR total_amount_clean <= 0 THEN 'Invalid amount'
        WHEN customer_id_clean IS NULL THEN 'Missing customer'
        WHEN order_date IS NULL THEN 'Missing date'
        ELSE 'Valid'
    END AS quality_flag,
    
    -- Audit
    GETDATE() AS etl_load_date,
    'silver.ecommerce_orders' AS etl_source

FROM parsed
WHERE order_date IS NOT NULL  -- Filter out null dates

INTO silver.ecommerce_orders  -- Uncomment to create table
