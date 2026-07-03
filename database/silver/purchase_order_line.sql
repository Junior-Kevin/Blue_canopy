WITH base AS (
    SELECT 
        [po_number],
        [line_number],
        [product_id],
        CAST([quantity_ordered] AS INT) AS quantity_ordered,
        ROUND(CAST([unit_price] AS FLOAT), 2) AS unit_price_kes,
        ROUND(CAST([line_total] AS FLOAT), 2) AS line_total_kes,
        
        -- Data validation
        ROUND(CAST([quantity_ordered] AS FLOAT) * CAST([unit_price] AS FLOAT), 2) AS calculated_line_total
        
    FROM [Blue_canopy].[bronze].[purchase_order_lines_raw]
    WHERE [po_number] IS NOT NULL 
        AND [product_id] IS NOT NULL
),

validated AS (
    SELECT 
        *,
        
        -- Validation flag
        CASE 
            WHEN quantity_ordered <= 0 THEN 'Invalid quantity'
            WHEN unit_price_kes <= 0 THEN 'Invalid unit price'
            WHEN line_total_kes <= 0 THEN 'Invalid line total'
            WHEN calculated_line_total != line_total_kes THEN 'Line total mismatch'
            ELSE 'Valid'
        END AS quality_flag,
        
        -- PO metadata
        LEFT(po_number, 2) AS po_prefix,
        TRY_CAST(RIGHT(po_number, 8) AS INT) AS po_sequence_number,
        
        -- Line metadata
        CONCAT(po_number, '_', line_number) AS po_line_key,
        
        -- Cost calculations
        ROUND(line_total_kes / NULLIF(quantity_ordered, 0), 2) AS calculated_unit_price,
        ROUND(line_total_kes * 1.10, 2) AS landed_cost_kes,  -- Assuming 10% landed cost
        ROUND(line_total_kes * 0.16, 2) AS estimated_vat_kes  -- 16% VAT in Kenya
        
    FROM base
)

SELECT 
    -- Surrogate key
    po_line_key,
    
    -- Dimensions
    po_number,
    line_number,
    product_id,
    
    -- Quantities and costs
    quantity_ordered,
    unit_price_kes,
    line_total_kes,
    
    -- Derived metrics
    calculated_unit_price,
    landed_cost_kes,
    estimated_vat_kes,
    
    -- PO metadata
    po_prefix,
    po_sequence_number,
    
    -- Quality
    quality_flag,
    
    -- Audit
    GETDATE() AS etl_load_date,
    'silver.purchase_order_lines' AS etl_source
    
INTO silver.purchase_order_lines
FROM validated
ORDER BY po_number, line_number
