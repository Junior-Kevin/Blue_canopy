WITH line_items AS (
    SELECT  
        [transaction_id],
        [line_number],
        [product_id],
        [quantity],
        [unit_price_kes],
        [discount_rate],
        [effective_unit_price_kes],
        [discount_amount_kes],
        [line_total_kes],
        [discount_tier]
    FROM [Blue_canopy].[silver].[pos_line_items] 
),

trans AS (
    SELECT  
        [transaction_id],
        [date],
        [time],
        [store_id],
        [customer_id],
        [cashier_id],
        [payment_method]  -- Added payment_method
    FROM [Blue_canopy].[silver].[pos_transactions]
),

Sales AS (
    SELECT
        -- Transaction identifiers
        l.[transaction_id],
        l.[line_number],
        
        -- Product
        l.[product_id],
        
        -- Store & people
        t.[store_id],
        t.[customer_id],
        t.[cashier_id],
        
        -- Date & time
        t.[date],
        t.[time],
        
        -- Payment
        t.[payment_method],  -- Added here
        
        -- Sales metrics
        l.[quantity],
        l.[unit_price_kes],
        l.[discount_rate],
        l.[effective_unit_price_kes],
        l.[discount_amount_kes],
        l.[line_total_kes],
        l.[discount_tier]
        
    FROM line_items l
    INNER JOIN trans t
        ON l.transaction_id = t.transaction_id
)

SELECT 
    -- Transaction identifiers
    s.[transaction_id],
    s.[line_number],
    
    -- Product
    s.[product_id],
    p.[product_name],        -- Assuming this exists
    p.[category],            -- Assuming this exists
    
    -- Store & people
    s.[store_id],
    s.[customer_id],
    s.[cashier_id],
    
    -- Date & time
    s.[date],
    s.[time],
    
    -- Payment
    s.[payment_method],      -- Final output includes it
    
    -- Product costs & margins
    p.[unit_cost_kes],
    p.[retail_price_kes],
    p.[margin_percentage],
    
    -- Sales metrics
    s.[quantity],
    s.[unit_price_kes],
    s.[discount_rate],
    s.[effective_unit_price_kes],
    s.[discount_amount_kes],
    s.[line_total_kes],
    s.[discount_tier],
    
    -- Optional: Calculated fields
    (s.[quantity] * s.[effective_unit_price_kes]) AS [effective_line_total_kes],
    (s.[line_total_kes] - (s.[quantity] * p.[unit_cost_kes])) AS [gross_profit_kes]
    
FROM Sales s
LEFT JOIN [Blue_canopy].[silver].[products] p
    ON s.product_id = p.product_id 
    AND s.date >= p.valid_from 
    AND (p.valid_to IS NULL OR s.date <= p.valid_to)
    
ORDER BY 
    s.[date] DESC,           -- Most recent first
    s.[transaction_id],
    s.[line_number],
    s.[product_id];
