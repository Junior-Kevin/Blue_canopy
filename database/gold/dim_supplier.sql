USE Blue_canopy;
GO
DROP TABLE IF EXISTS gold.dim_supplier;
GO
SELECT 
       ROW_NUMBER() OVER(order by supplier_id)  supplier_sk
       ,[supplier_id]
      ,[supplier_name]
	  ,[category]
      ,[contact_person]
      ,[phone]
      ,REPLACE([email],'contact',
	   LOWER(CONCAT(LEFT(supplier_name,1),
	     SUBSTRING(supplier_name,CHARINDEX(' ',supplier_name)+1,1)))) email
      ,[valid_from]
      ,[payment_terms]
      ,[lead_time_days]
      ,[tax_id]
	  INTO gold.dim_supplier
  FROM [Blue_canopy].[silver].[suppliers]
