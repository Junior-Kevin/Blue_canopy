SELECT top 50
    sales_id = CONCAT('POS-', CAST(t.transaction_id AS VARCHAR), '-', CAST(pl.line_number AS VARCHAR)),
	transaction_id = t.transaction_id,
    date_sk = t.date,
    customer_sk = t.customer_id,
    product_sk = pl.product_id,
    store_sk  = t.store_id,
	employee_sk = cashier_id,
    promotion_id = p.promotion_id,
	campaign_sk = ca.campaign_id,
    quantity = pl.quantity,
    unit_price = pl.unit_price_kes,
    discount_amount = pl.discount_amount_kes,
    line_total = pl.line_total_kes,
    delivery_fee = 0,
    payment_method = t.payment_method,
    transaction_type = 'POS',
    net_sales = pl.line_total_kes,
    net_quantity = pl.quantity
FROM silver.pos_transactions t
INNER JOIN silver.pos_line_items pl ON t.transaction_id = pl.transaction_id
LEFT JOIN [Blue_canopy].[silver].[promotions]  P
ON t.date between p.start_date AND p.end_date
LEFT JOIN [Blue_canopy].[silver].[campaigns] ca
ON t.date between ca.start_date and ca.end_date
