USE Blue_canopy;
GO
DROP TABLE IF EXISTS silver.ecommerce_order_lines
GO
WITH base AS (
    SELECT 
        [order_id],
        [line_number],
        [product_id],
        CAST([quantity] AS INT) AS quantity,
        ABS(ROUND(CAST([unit_price] AS FLOAT), 2)) AS unit_price_kes,
        ABS(CAST([discount_rate] AS FLOAT)) AS discount_rate,
        ABS(ROUND(CAST([line_total] AS FLOAT), 2)) AS line_total_kes
    FROM [Blue_canopy].[bronze].[ecommerce_order_lines_raw]
    WHERE [order_id] IS NOT NULL 
        AND [product_id] IS NOT NULL
),

validated AS (
    SELECT 
        *,
        
        -- Calculate expected line total (validation)
        ROUND(quantity * unit_price_kes * (1 - discount_rate), 2) AS calculated_line_total,
        
        -- Calculate discount amount
        ROUND(quantity * unit_price_kes * discount_rate, 2) AS discount_amount_kes,
        
        -- Calculate unit price after discount
        ROUND(unit_price_kes * (1 - discount_rate), 2) AS unit_price_after_discount_kes,
        
        -- Extract order prefix and sequence
        LEFT(order_id, 4) AS order_prefix,
        TRY_CAST(RIGHT(order_id, 8) AS INT) AS order_sequence_number,
        
        -- Discount tier categorization
        CASE 
            WHEN discount_rate = 0 THEN 'No Discount'
            WHEN discount_rate < 0.05 THEN 'Small Discount (<5%)'
            WHEN discount_rate < 0.10 THEN 'Standard Discount (5-10%)'
            WHEN discount_rate < 0.20 THEN 'Large Discount (10-20%)'
            ELSE 'Heavy Discount (>20%)'
        END AS discount_tier,
        
        -- Data quality flag
        CASE 
            WHEN quantity <= 0 THEN 'Invalid quantity'
            WHEN unit_price_kes <= 0 THEN 'Invalid unit price'
            WHEN discount_rate < 0 OR discount_rate > 1 THEN 'Invalid discount rate'
            WHEN line_total_kes <= 0 THEN 'Invalid line total'
           -- WHEN ROUND(quantity * unit_price_kes * (1 - discount_rate), 2) != line_total_kes 
                --THEN 'Line total mismatch'
            ELSE 'Valid'
        END AS quality_flag
        
    FROM base
)
SELECT 
    -- Surrogate key
    ROW_NUMBER() OVER(ORDER BY order_id)  AS order_line_key,
    
    -- Dimensions
    order_id,
    line_number,
    product_id,
    
    -- Quantities and pricing
    quantity,
    unit_price_kes,
    discount_rate,
    unit_price_after_discount_kes,
    discount_amount_kes,
    line_total_kes,
    
    -- Validation
    calculated_line_total,
    
    -- Categorizations
    discount_tier,
    order_prefix,
    order_sequence_number,
    
    -- Quality
    quality_flag,
    
    -- Audit
    GETDATE() AS etl_load_date,
    'silver.ecommerce_order_lines' AS etl_source
    
INTO silver.ecommerce_order_lines
FROM validated
WHERE quality_flag = 'Valid'  -- Only include valid records
ORDER BY order_id, line_number
