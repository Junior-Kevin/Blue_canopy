DROP TABLE IF EXISTS gold.fact_sales;
GO

SELECT *
INTO gold.fact_sales
FROM (
    SELECT 
           l.[transaction_id]
          ,[line_number]
          ,[customer_id]
          ,l.[product_id]
          ,[supplier_id]
          ,[store_id]
          ,[cashier_id]
          ,[transaction_date]
          ,[transaction_time]
          ,[delivery_address] = NULL
          ,[delivery_fee] = 0
          ,payment_method
          ,[order_status] = 'delivered'
          ,[quantity]
          ,[unit_price_kes]
          ,[discount_rate]
          ,[discount_amount_kes]
          ,[unit_price_after_discount_kes] = [effective_unit_price_kes]
          ,l.[line_total]
          ,[unit_cost_kes]
          ,[discount_tier]
    FROM [Blue_canopy].[silver].[pos_line_items] l
    INNER JOIN [Blue_canopy].[silver].[pos_transactions] t
    ON l.transaction_id = t.transaction_id
    LEFT JOIN [Blue_canopy].[gold].[dim_product] P
    ON l.[product_id] = p.product_id AND t.transaction_date BETWEEN P.valid_from AND p.valid_to
    
    UNION ALL
    
    SELECT
           [transaction_id] = [order_id]
          ,[line_number]
          ,[customer_id]
          ,[product_id]
          ,[supplier_id]
          ,store_id
          ,[cashier_id] = NULL
          ,[transaction_date] = [order_date]
          ,[transaction_time] = [order_time]
          ,[delivery_address]
          ,[delivery_fee]
          ,[payment_method]
          ,[order_status]
          ,[quantity]
          ,[unit_price_kes]
          ,[discount_rate]
          ,[discount_amount_kes]
          ,[unit_price_after_discount_kes]
          ,line_total = [line_total_kes]
          ,[unit_cost_kes]
          ,[discount_tier] 
    FROM (
        SELECT ROW_NUMBER() OVER(PARTITION BY order_line_key ORDER BY order_date, ol.order_id, line_number) AS order_line_key_new
              ,[order_line_key]
              ,ol.[order_id]
              ,[line_number]
              ,ol.[product_id]
              ,[supplier_id]
              ,eo.[customer_id]
              ,store_id = cst.primary_store_id
              ,[order_date]
              ,[order_time]
              ,[delivery_address]
              ,[delivery_fee]
              ,[payment_method]
              ,[order_status]
              ,[quantity]
              ,[unit_price_kes]
              ,[discount_rate]
              ,[discount_amount_kes]
              ,[unit_price_after_discount_kes]
              ,[line_total_kes]
              ,[unit_cost_kes]
              ,[discount_tier]
        FROM [Blue_canopy].[silver].[ecommerce_order_lines] ol
        LEFT JOIN [Blue_canopy].[silver].[ecommerce_orders] eo
        ON ol.order_id = eo.order_id
        LEFT JOIN [Blue_canopy].[gold].[dim_product] P
        ON ol.product_id = P.product_id AND order_date BETWEEN valid_from AND valid_to
        LEFT JOIN [Blue_canopy].[gold].[dim_customers] dc
        ON eo.customer_id = dc.customer_id
        LEFT JOIN [Blue_canopy].[silver].[cust_county] cst
        ON eo.customer_id = cst.customer_id
    ) t
    WHERE order_line_key_new = 1
) combined;
