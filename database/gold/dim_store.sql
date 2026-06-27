
USE Blue_canopy;
GO
DROP TABLE IF EXISTS gold.dim_store;
GO
SELECT store_sk = ROW_NUMBER() OVER(ORDER BY s.store_id)  
       ,s.[store_id]
      ,s.[store_name]
	  ,s.[county]
	  ,s.[town]
      ,s.[format]
      ,s.[size_sqm]
      ,s.[opening_date]
      ,s.[closing_date]
	  ,s.[is_active]
	  ,c.latitude 
	  ,c.longitude
	  ,c.population
	  ,c.avg_income_kes
	  ,sr.[valid_from]
      ,sr.[valid_to]
	  ,is_current_version = CASE WHEN sr.[valid_to] IS NULL THEN 'yes' ELSE 'no' END
	  INTO gold.dim_store
  FROM [Blue_canopy].[silver].[stores] s
 LEFT JOIN [Blue_canopy].[silver].[gis_counties] c
 ON s.county = c.county
 LEFT JOIN [Blue_canopy].[bronze].[stores_raw] sr
 ON s.store_id = sr.store_id
 ORDER BY store_id

