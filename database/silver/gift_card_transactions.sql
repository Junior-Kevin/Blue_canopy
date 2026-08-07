DROP TABLE IF EXISTS silver.gift_card_transactions;
SELECT 
       [transaction_id]
      ,[card_number]
      ,CAST([date]AS DATE) date
      ,CAST([amount] AS INT) amount 
      ,[type]
INTO silver.gift_card_transactions
FROM [Blue_canopy].[bronze].[gift_card_transactions_raw]
