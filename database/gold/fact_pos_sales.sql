USE Blue_canopy;
GO
DROP TABLE IF EXISTS pos_fact_sales;
GO
SELECT [pos_line_key]
      ,l.[transaction_id]
      ,[line_number]
      ,l.[product_id]
	  ,[store_id]
      ,[customer_id]
      ,[cashier_id]
	  ,[supplier_id]
      ,[transaction_date]
      ,[transaction_time]
      ,[quantity]
      ,[unit_price_kes]
      ,[discount_rate]
      ,[effective_unit_price_kes]
      ,[discount_amount_kes]
      ,[line_total_kes]
      ,[unit_cost_kes]
      ,[retail_price_kes]
      ,[discount_tier]
      ,[line_value_tier]
	  INTO pos_fact_sales
  FROM [Blue_canopy].[silver].[pos_line_items] l
  INNER JOIN [Blue_canopy].[silver].[pos_transactions] t
  ON l.transaction_id = t.transaction_id
  LEFT JOIN [Blue_canopy].[gold].[dim_product] P
  ON l.[product_id] = p.product_id AND t.transaction_date BETWEEN P.valid_from AND p.valid_to
  ORDER BY transaction_date,transaction_id,line_number;
  GO
  CREATE NONCLUSTERED INDEX idx_product_id_posfact
  ON pos_fact_sales (product_id);
  GO
  CREATE NONCLUSTERED  INDEX idx_store_id_posfact
  ON pos_fact_sales (store_id);
  GO
  CREATE NONCLUSTERED INDEX idx_customer_id_posfact
  ON pos_fact_sales (customer_id);
