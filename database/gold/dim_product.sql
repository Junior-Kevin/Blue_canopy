
DROP TABLE IF EXISTS gold.dim_product;
SELECT  
       product_sk  = CONVERT(INT,ROW_NUMBER() OVER (ORDER BY product_id, valid_from))
	  ,product_id
	  ,[product_name]
	  ,[brand]
      ,[category]
      ,[subcategory]
	  ,[supplier_id]
	  ,[unit_cost_kes]
      ,[retail_price_kes]
      ,[margin_percentage]
	  ,margin_band =  CASE
	                      WHEN margin_percentage < 20 THEN 'low'
						  WHEN margin_percentage Between 20 and 40  THEN 'medium' 
						  WHEN margin_percentage > 40 THEN 'high'
					  END
      ,[introduction_date]
	  ,[is_active] 
      ,[valid_from]
      ,[valid_to]
      ,[discontinued_date]
	  ,is_current_version = CASE WHEN valid_to IS NULL THEN 'yes' ELSE 'no' END
	  INTO gold_dim_product
  FROM [Blue_canopy].[silver].[products]
  ORDER BY product_id
