-- Create the sales table from the query results
SELECT 
    sales_id = CONCAT('ECO-', CAST(o.order_id AS VARCHAR), '-', CAST(ol.line_number AS VARCHAR)),
    transaction_date = o.order_date,
    customer_id = o.customer_id,
    product_id = ol.product_id,
    store_id = NULL,  -- E-commerce has no store
    promotion_id = NULL,  -- Join if you have promotion mapping
    quantity = ol.quantity,
    unit_price = ol.unit_price_kes,
    discount_amount = ol.discount_amount_kes,
    line_total = ol.line_total_kes,
    delivery_fee = o.delivery_fee,
    payment_method = o.payment_method,
    transaction_type = 'ECOMMERCE',
    return_flag = 0,
    return_quantity = 0,
    return_amount = 0,
    net_sales = ol.line_total_kes,
    net_quantity = ol.quantity
--INTO gold.sales_fact  -- This creates the table
FROM silver.ecommerce_orders o
INNER JOIN silver.ecommerce_order_lines ol ON o.order_id = ol.order_id
WHERE o.status NOT IN ('cancelled', 'refunded')

UNION ALL

-- POS sales
SELECT 
    sales_id = CONCAT('POS-', CAST(t.transaction_id AS VARCHAR), '-', CAST(pl.line_number AS VARCHAR)),
    transaction_date = t.date,
    customer_id = t.customer_id,
    product_id = pl.product_id,
    store_id = t.store_id,
    promotion_id = NULL,
    quantity = pl.quantity,
    unit_price = pl.unit_price_kes,
    discount_amount = pl.discount_amount_kes,
    line_total = pl.line_total_kes,
    delivery_fee = 0,
    payment_method = t.payment_method,
    transaction_type = 'POS',
    return_flag = 0,
    return_quantity = 0,
    return_amount = 0,
    net_sales = pl.line_total_kes,
    net_quantity = pl.quantity
FROM silver.pos_transactions t
INNER JOIN silver.pos_line_items pl ON t.transaction_id = pl.transaction_id

UNION ALL

-- Returns (negative sales)
SELECT 
    sales_id = CONCAT('RET-', CAST(r.return_id AS VARCHAR)),
    transaction_date = r.return_date,  -- Fixed column name to match other selects
    customer_id = t.customer_id,  
    product_id = r.product_id,
    store_id = t.store_id,
    promotion_id = NULL,
    quantity = -r.quantity_returned,
    unit_price = r.refund_amount / NULLIF(r.quantity_returned, 0),
    discount_amount = 0,
    line_total = -r.refund_amount,
    delivery_fee = 0,
    payment_method = 'RETURN',
    transaction_type = 'RETURN',
    return_flag = 1,
    return_quantity = r.quantity_returned,
    return_amount = r.refund_amount,
    net_sales = -r.refund_amount,
    net_quantity = -r.quantity_returned
  -- Note: INTO only appears ONCE, at the end of the first SELECT
FROM silver.returns r
LEFT JOIN silver.pos_transactions t
ON r.original_transaction_id = t.transaction_id;
