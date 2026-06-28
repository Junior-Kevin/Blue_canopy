USE Blue_canopy;
GO
DROP TABLE IF EXISTS gold.dim_campaign;
GO
SELECT  campaign_sk  = ROW_NUMBER() OVER(ORDER BY campaign_id)
       ,[campaign_id]
      ,[campaign_name]
      ,[campaign_type]
      ,[channel]
      ,[start_date]
      ,[end_date]
	  ,[campaign_duration_days]
      ,[budget_kes]
      ,[actual_spend_kes]
	  ,budget_utilisation_pct =CAST(actual_spend_kes AS FLOAT)/budget_kes*100
      ,[discount_rate]
	  INTO  gold.dim_campaign
  FROM [Blue_canopy].[silver].[campaigns]

