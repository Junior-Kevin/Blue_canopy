
USE Blue_canopy;
GO
DROP TABLE IF EXISTS gold.dim_store;
GO
SELECT  [store_key]
      ,[store_id]
      ,[valid_from]
      ,[valid_to]
      ,[store_name]
      ,[county]
      ,[town]
      ,[format]
      ,[size_sqm]
      ,[opening_date]
      ,[closing_date]
      ,[is_active]
	  INTO gold.dim_store
  FROM [Blue_canopy].[silver].[stores]

