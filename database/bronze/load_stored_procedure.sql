-- ====================================================================
-- Stored Procedure: bronze.load_bronze_tables (Simplified Working Version)
-- Purpose: Bulk load all CSV files from the bronze layer into SQL Server.
--          Uses the exact options that have been tested and work.
-- Parameters:
--   @data_root       – base path to the bronze folder (trailing backslash required)
--   @truncate_first  – whether to truncate tables before loading
--   @continue_on_error – if 1, continue after an error; otherwise stop
-- ====================================================================
CREATE OR ALTER PROCEDURE bronze.load_bronze_tables
    @data_root NVARCHAR(255) = 'C:\Users\HomePC\Desktop\data science\Market Basket Analysis\bronze\Scripts\new_scripts\bronze\',
    @truncate_first BIT = 1,
    @continue_on_error BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    -- ====================================================================
    -- 1. Define the mapping between tables and their relative CSV file paths
    -- ====================================================================
    DECLARE @files TABLE (
        table_schema NVARCHAR(128) DEFAULT 'bronze',
        table_name NVARCHAR(128),
        relative_path NVARCHAR(500)
    );

    INSERT INTO @files (table_name, relative_path) VALUES
        -- competitive_intelligence
        ('competitors_raw',              'competitive_intelligence\competitors_raw.csv'),
        ('competitor_quarterly_raw',     'competitive_intelligence\competitor_quarterly_raw.csv'),
        ('competitor_stores_raw',        'competitive_intelligence\competitor_stores_raw.csv'),
        -- crm
        ('crm_raw',                      'crm\crm_raw.csv'),
        -- customer_service
        ('feedback_raw',                 'customer_service\feedback_raw.csv'),
        ('service_interactions_raw',     'customer_service\service_interactions_raw.csv'),
        -- finance
        ('gl_transactions_raw',          'finance\gl_transactions_raw.csv'),
        ('store_daily_financials_raw',   'finance\store_daily_financials_raw.csv'),
        -- gis
        ('gis_counties_raw',             'gis\gis_counties_raw.csv'),
        ('gis_locations_raw',             'gis\gis_locations_raw.csv'),
        -- hr
        ('employee_shifts_raw',           'hr\employee_shifts_raw.csv'),
        ('hr_raw',                        'hr\hr_raw.csv'),
        ('time_tracking_raw',             'hr\time_tracking_raw.csv'),
        -- inventory
        ('inventory_movements_raw',       'inventory\inventory_movements_raw.csv'),
        ('inventory_snapshots_raw',       'inventory\inventory_snapshots_raw.csv'),
        -- loyalty
        ('loyalty_transactions_raw',      'loyalty\loyalty_transactions_raw.csv'),
        -- macroeconomic
        ('economic_raw',                  'macroeconomic\economic_raw.csv'),
        -- marketing
        ('campaigns_raw',                 'marketing\campaigns_raw.csv'),
        ('promotions_raw',                 'marketing\promotions_raw.csv'),
        ('promotion_products_raw',         'marketing\promotion_products_raw.csv'),
        -- procurement
        ('goods_receipts_raw',             'procurement\goods_receipts_raw.csv'),
        ('purchase_orders_raw',            'procurement\purchase_orders_raw.csv'),
        ('purchase_order_lines_raw',       'procurement\purchase_order_lines_raw.csv'),
        -- products
        ('products_raw',                   'products\products_raw.csv'),
        -- sales
        ('ecommerce_orders_raw',           'sales\ecommerce_orders_raw.csv'),
        ('ecommerce_order_lines_raw',      'sales\ecommerce_order_lines_raw.csv'),
        ('gift_cards_raw',                 'sales\gift_cards_raw.csv'),
        ('gift_card_transactions_raw',     'sales\gift_card_transactions_raw.csv'),
        ('pos_line_items_raw',              'sales\pos_line_items_raw.csv'),
        ('pos_transactions_raw',            'sales\pos_transactions_raw.csv'),
        ('returns_raw',                     'sales\returns_raw.csv'),
        -- stores
        ('stores_raw',                      'stores\stores_raw.csv'),
        -- suppliers
        ('suppliers_raw',                    'suppliers\suppliers_raw.csv');

    -- ====================================================================
    -- 2. Declare variables for cursor and dynamic SQL
    -- ====================================================================
    DECLARE @schema NVARCHAR(128), @table NVARCHAR(128), @rel_path NVARCHAR(500), @full_path NVARCHAR(500);
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @error_occurred BIT = 0;

    DECLARE file_cursor CURSOR FOR
        SELECT table_schema, table_name, relative_path
        FROM @files;

    OPEN file_cursor;
    FETCH NEXT FROM file_cursor INTO @schema, @table, @rel_path;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Build full file path
        SET @full_path = @data_root + @rel_path;

        BEGIN TRY
            -- Optional: truncate table first
            IF @truncate_first = 1
            BEGIN
                SET @sql = N'TRUNCATE TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N';';
                EXEC sp_executesql @sql;
                PRINT 'Truncated ' + @schema + '.' + @table;
            END

            -- Build BULK INSERT statement with proven options
            SET @sql = N'
BULK INSERT ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N'
FROM ''' + @full_path + N'''
WITH (
    FIRSTROW = 2,                -- skip header row
    FIELDTERMINATOR = '','',
    ROWTERMINATOR = ''\n'',
    TABLOCK                       -- improves performance
);';

            EXEC sp_executesql @sql;
            PRINT 'Loaded ' + @schema + '.' + @table + ' from ' + @full_path;
        END TRY
        BEGIN CATCH
            PRINT 'ERROR loading ' + @schema + '.' + @table + ': ' + ERROR_MESSAGE();
            SET @error_occurred = 1;
            IF @continue_on_error = 0
            BEGIN
                -- Stop processing on first error
                BREAK;
            END
        END CATCH

        FETCH NEXT FROM file_cursor INTO @schema, @table, @rel_path;
    END

    CLOSE file_cursor;
    DEALLOCATE file_cursor;

    -- ====================================================================
    -- 3. Final summary
    -- ====================================================================
    PRINT '==============================================';
    IF @error_occurred = 0
        PRINT 'Bronze load completed successfully.';
    ELSE
        PRINT 'Bronze load completed with errors.';
    PRINT '==============================================';
END;
GO

EXEC bronze.load_bronze_tables;
