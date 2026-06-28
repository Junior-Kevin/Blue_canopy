USE Blue_canopy;
GO
DROP TABLE IF EXISTS gold.dim_geography_economic;
GO
WITH econ AS(
SELECT ROW_NUMBER() OVER(PARTITION BY county ORDER BY date)flag
      ,[economic_id]
      ,[county]
	  ,MAX(date) OVER(PARTITION BY county ORDER BY county)flag_date
      ,[date]
      ,[qtr]
      ,[month]
      ,[gdp_growth_pct]
      ,[inflation_pct]
      ,[unemployment_pct]
      ,[consumer_confidence]
      ,[retail_sales_index]
      ,[fuel_price_kes]
      ,[usd_kes_rate]
  FROM [Blue_canopy].[silver].[economic]
  ),econ_flag AS (SELECT * FROM econ WHERE date = flag_date)
  SELECT ROW_NUMBER() OVER(ORDER BY C.county) geo_sk 
       ,C.[county]
      ,[population]
      ,[avg_income_kes]
      ,[latitude]
      ,[longitude]
	    ,[gdp_growth_pct]
      ,[inflation_pct]
      ,[unemployment_pct]
      ,[consumer_confidence]
      ,[retail_sales_index]
      ,[fuel_price_kes]
      ,[usd_kes_rate]
	  ,economic_as_of_date= E.date
	  INTO gold.dim_geography_economic
  FROM [Blue_canopy].[silver].[gis_counties] C
  LEFT JOIN econ_flag E ON 
  C.county = E.county
