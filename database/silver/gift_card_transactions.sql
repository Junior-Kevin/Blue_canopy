DROP TABLE IF EXISTS silver.gift_card_transactions;
GO
SELECT 
       [transaction_id]
      ,[card_number]
      ,CAST([date] AS DATE) date
      ,CAST([amount] AS INT)amount 
      ,SUBSTRING([type],1,CHARINDEX(',',type)-1) type
	  ,SUBSTRING(type,CHARINDEX(',',type)+1,20) linked_transaction_id
INTO gift_card_transactions
FROM [Blue_canopy].[bronze].[gift_card_transactions_raw]
