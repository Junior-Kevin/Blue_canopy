DROP TABLE IF EXISTS Blue_canopy.gold.dim_customers;
GO
SELECT  ROW_NUMBER() OVER(order by registration_date) customer_sk 
       ,crm.[customer_id]
      ,[first_name]
      ,[last_name]
      ,[full_name]
      ,[gender]
      ,[birth_date]
	  ,CASE 
	        WHEN YEAR(birth_date) BETWEEN 2013 AND YEAR(GETDATE()) THEN 'Gen Alpha'
			WHEN YEAR(birth_date) BETWEEN 1997 AND 2012 THEN 'Gen Z'
			WHEN YEAR(birth_date) BETWEEN 1981 AND 1996 THEN 'Millennial'
			WHEN YEAR(birth_date) BETWEEN 1965 AND 1980 THEN 'Gen X'
			WHEN YEAR(birth_date) BETWEEN 1945 AND 1964 THEN 'Boomer'
			ELSE 'Other/Unknown'
		END AS generation
      ,[age]
      ,[age_band]
      ,[phone]
      ,[email]
      ,CASE 
	       WHEN cc.[home_county] IS NULL THEN crm.county
		   ELSE cc.[home_county]
		END AS home_county
      ,cc.[primary_store_id]
	  ,cc.visits_to_primary_store
      ,[town]
      ,[customer_segment]
      ,[acquisition_channel]
      ,[registration_date]
      ,[churn_date]
      ,[loyalty_tier]
      ,[communication_preferences]
      ,[feedback_score]
      ,[is_churned]
      ,[tenure_days]
      ,[tenure_months]
      ,[tenure_band]
      ,[registration_year]
      ,[registration_month]
      ,[registration_quarter]
      ,[email_domain]
      ,[phone_prefix]
      ,[is_phone_valid]
      ,[is_email_valid]
   INTO gold.dim_customers
   FROM [Blue_canopy].[silver].[crm] crm 
   LEFT JOIN [Blue_canopy].[silver].[cust_county] cc
   ON crm.customer_id = cc.customer_id





