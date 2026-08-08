DROP TABLE IF EXISTS silver.gift_cards;
WITH cards AS (
SELECT [card_number]
      ,customer_id = [issue_date]
      ,[issue_date]= [expiry_date]
      ,[expiry_date]= [initial_balance]
      ,SUBSTRING(current_balance,1,CHARINDEX(',',current_balance)-1) initial_balance
	  ,SUBSTRING(current_balance,CHARINDEX(',',current_balance)+1,100) transactions
  FROM [Blue_canopy].[bronze].[gift_cards_raw])
  SELECT card_number
       ,[issue_date]
      ,[expiry_date]
      ,[initial_balance]
	  ,current_balance=SUBSTRING(transactions,1,CHARINDEX(',',transactions)-1)
	  ,transactions = REPLACE(SUBSTRING(transactions,CHARINDEX(',',transactions)+1,100),'"','')
	INTO silver.gift_cards
	FROM cards
