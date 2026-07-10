USE Blue_canopy;
GO
DROP TABLE IF EXISTS silver.pos_line_items;
GO
WITH base AS (
    SELECT
        REPLACE(TRIM([transaction_id]),' ' ,'') AS transaction_id,  -- Remove spaces
        [line_number],
        CASE
		     WHEN [product_id] LIKE '%DUP' THEN SUBSTRING(product_id,1,CHARINDEX('D',product_id,5)-2)
			 ELSE [product_id] END AS product_id,
        ABS(CAST([quantity] AS INT)) AS quantity,
        ROUND(ABS(CAST([unit_price] AS FLOAT)), 2) AS unit_price_kes,
        ABS(CAST([discount_rate] AS FLOAT)) AS discount_rate,
        ROUND(ABS(CAST([line_total] AS FLOAT)), 2) AS line_total_kes
    FROM [Blue_canopy].[bronze].[pos_line_items_raw]
    WHERE [transaction_id] IS NOT NULL 
      AND [transaction_id] != ''
      AND [product_id] IS NOT NULL
),
validated AS (
    SELECT 
        -- Core identifiers
        transaction_id,
        line_number,
        product_id,
        
        -- Quantities and pricing
        quantity,
        unit_price_kes,
        discount_rate,
        line_total_kes,
        
        -- Calculate expected line total (validation)
        ROUND(quantity * unit_price_kes * (1 - discount_rate), 2) AS calculated_line_total,
        
        -- Calculate discount amount
        ROUND(quantity * unit_price_kes * discount_rate, 2) AS discount_amount_kes,
        
        -- Calculate unit price after discount
        ROUND(unit_price_kes * (1 - discount_rate), 2) AS effective_unit_price_kes,
        
        -- Discount tier categorization
        CASE 
            WHEN discount_rate = 0 THEN 'No Discount'
            WHEN discount_rate < 0.05 THEN 'Small Discount (<5%)'
            WHEN discount_rate < 0.10 THEN 'Standard Discount (5-10%)'
            WHEN discount_rate < 0.20 THEN 'Large Discount (10-20%)'
            ELSE 'Heavy Discount (>20%)'
        END AS discount_tier,
        
        -- Line value tier
        CASE 
            WHEN line_total_kes >= 500000 THEN 'Premium Line (500K+ KES)'
            WHEN line_total_kes >= 100000 THEN 'High Value Line (100K-500K)'
            WHEN line_total_kes >= 50000 THEN 'Medium Value Line (50K-100K)'
            WHEN line_total_kes >= 10000 THEN 'Low Value Line (10K-50K)'
            ELSE 'Small Item (<10K KES)'
        END AS line_value_tier,
        
        -- Extract transaction prefix and sequence
        CASE 
            WHEN transaction_id LIKE 'TXN-%' THEN 'POS'
            WHEN transaction_id LIKE '%-%' THEN LEFT(transaction_id, CHARINDEX('-', transaction_id) - 1)
            ELSE 'Unknown'
        END AS transaction_source,
        
        -- Data quality flag
        CASE 
            WHEN quantity <= 0 THEN 'Invalid quantity'
            WHEN unit_price_kes <= 0 THEN 'Invalid unit price'
            WHEN discount_rate < 0 OR discount_rate > 1 THEN 'Invalid discount rate'
            WHEN line_total_kes <= 0 THEN 'Invalid line total'
            WHEN ABS(ROUND(quantity * unit_price_kes * (1 - discount_rate), 2) - line_total_kes) > 0.01 THEN 'Line total mismatch'
            WHEN transaction_id LIKE '% %' OR transaction_id = '' THEN 'Malformed transaction ID'
            ELSE 'Valid'
        END AS quality_flag
        
    FROM base
)

SELECT 
    -- Surrogate key
    ROW_NUMBER() OVER(ORDER BY transaction_id)AS pos_line_key,
    -- Foreign keys
    transaction_id,
    line_number,
    product_id,
    --Quantities and pricing
    quantity,
    unit_price_kes,
    discount_rate,
    effective_unit_price_kes,
    discount_amount_kes,
    line_total_kes,
    -- Categorizations
    discount_tier,
    line_value_tier,
    transaction_source,
    -- Audit
    GETDATE() AS etl_load_date,
    'silver.pos_line_items' AS etl_source
INTO silver.pos_line_items
FROM validated
WHERE quality_flag = 'Valid'
ORDER BY transaction_id, line_number;
GO
DROP INDEX IF EXISTS idx_pos_line_items_poslinekey ON silver.pos_line_items;
GO
CREATE CLUSTERED COLUMNSTORE INDEX idx_pos_line_items_poslinekey ON
silver.pos_line_items;
GO
DROP INDEX IF EXISTS idx_poslineitemstransaction_id ON silver.pos_line_items;
GO
CREATE NONCLUSTERED INDEX idx_poslineitemstransaction_id 
ON silver.pos_line_items (transaction_id);
