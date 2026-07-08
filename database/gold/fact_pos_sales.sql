USE Blue_canopy;
GO
DROP TABLE IF EXISTS gold.fact_pos_sales;
GO
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
        [payment_method]
    FROM [Blue_canopy].[silver].[pos_transactions]
),

Sales AS (
    SELECT
        l.[transaction_id],
        l.[line_number],
        l.[product_id],
        t.[store_id],
        t.[customer_id],
        t.[cashier_id],
        t.[date],
        t.[time],
        t.[payment_method], 
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
),

-- Get the product matching the transaction date
sales_with_valid_product AS (
    SELECT 
        s.[transaction_id],
        s.[line_number],
        s.[product_id],
        s.[store_id],
        s.[customer_id],
        s.[cashier_id],
        s.[date],
        s.[time],
        s.[payment_method],    
        p.[unit_cost_kes],
        p.[retail_price_kes],
        p.[margin_percentage],
        s.[quantity],
        s.[unit_price_kes],
        s.[discount_rate],
        s.[effective_unit_price_kes],
        s.[discount_amount_kes],
        s.[line_total_kes]
    FROM Sales s
    LEFT JOIN [Blue_canopy].[silver].[products] p
        ON s.product_id = p.product_id 
        AND s.date >= p.valid_from 
        AND (p.valid_to IS NULL OR s.date <= p.valid_to)
		WHERE s.[product_id] NOT LIKE '%DUP'
),
-- Get the latest product record for each product as fallback
latest_products AS (
    SELECT 
        product_id,
        unit_cost_kes,
        retail_price_kes,
        margin_percentage
    FROM (
        SELECT 
            ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY valid_from DESC) as rn,
            [product_id],
            [unit_cost_kes],
            [retail_price_kes],
            [margin_percentage]
        FROM [Blue_canopy].[silver].[products]
		WHERE product_id NOT LIKE '%DUP'
        -- Include both active and historical products
    ) t 
    WHERE rn = 1
),

-- Get the first/earliest product record as another fallback option
first_products AS (
    SELECT 
        product_id,
        unit_cost_kes,
        retail_price_kes,
        margin_percentage
    FROM (
        SELECT 
            ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY valid_from ASC) as rn,
            [product_id],
            [unit_cost_kes],
            [retail_price_kes],
            [margin_percentage]
        FROM [Blue_canopy].[silver].[products]

    ) t 
    WHERE rn = 1
)

SELECT pos_sales_sk = ROW_NUMBER() OVER(ORDER BY s.[transaction_id],s.[date]),
    s.[transaction_id],
    s.[line_number],
    s.[product_id],
    s.[store_id],
    s.[customer_id],
    s.[cashier_id],
    s.[date],
    s.[time],
    s.[payment_method],    
    -- Try date-matched product, then latest product, then first product
    COALESCE(s.[unit_cost_kes], lp.[unit_cost_kes], fp.[unit_cost_kes]) AS unit_cost_kes,
    COALESCE(s.[retail_price_kes], lp.[retail_price_kes], fp.[retail_price_kes]) AS retail_price_kes,
    COALESCE(s.[margin_percentage], lp.[margin_percentage], fp.[margin_percentage]) AS margin_percentage,
    s.[quantity],
    s.[unit_price_kes],
    s.[discount_rate],
    s.[effective_unit_price_kes],
    s.[discount_amount_kes],
    s.[line_total_kes],
    -- Use COALESCE here too for the gross profit calculation
    (s.[line_total_kes] - (s.[quantity] * COALESCE(s.[unit_cost_kes], lp.[unit_cost_kes], fp.[unit_cost_kes]))) AS gross_profit_kes
INTO  gold.fact_pos_sales
FROM sales_with_valid_product s
LEFT JOIN latest_products lp
    ON s.product_id = lp.product_id
LEFT JOIN first_products fp
    ON s.product_id = fp.product_id
ORDER BY 
    s.[date] DESC,      
    s.[transaction_id],
    s.[line_number],
    s.[product_id];
