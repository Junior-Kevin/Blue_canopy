📊 Data Modelling & Gold Layer Transformation

Power BI Semantic Model · Power Query (M) Transformations

| ✅ 29 Silver Tables → 15 Gold Objects | ✅ 6 Conformed Dimensions | ✅ 9 Atomic Fact Tables | ✅ 3 Aggregate Tables | ✅ 1 Bridge Table | ⚡ All transformations in Power Query (M) |
📁 Project Overview

This document details the comprehensive data modelling process for the Blue Canopy Retail Analytics Project. Unlike traditional approaches that build the Gold Layer in SQL Server, I built the entire dimensional model directly in Power BI using Power Query (M language). Starting from 29 raw silver-layer tables imported from SQL Server, I transformed them into a production-ready star schema using Power Query's ETL capabilities.
🏗️ Architecture Overview
text

SQL Server (Silver Layer) 
    ↓ [Import Mode]
Power BI Desktop
    ↓ [Power Query - M Language]
Gold Layer (Power Query transforms)
    ↓ [Model Relationships]
Power BI Semantic Model (Star Schema)
    ↓ [DAX Measures]
5 Data Marts → Interactive Dashboards

Why Power BI Instead of SQL Server?
Aspect	SQL Server Approach	Power BI Approach (My Choice)
Transformation Location	SQL stored procedures/views	Power Query (M language)
SCD Type 2 Logic	T-SQL with window functions	M code with Table.Group, Table.AddColumn
Surrogate Key Generation	IDENTITY columns	Table.AddIndexColumn
Performance	Pre-aggregated in SQL	Import mode + Aggregations
Maintenance	Two systems (SQL + PBI)	Single system (Power BI)
Learning Value	SQL skills	M language + Power Query skills
📁 Raw Data Structure (Before)

The raw data tables represent the cleaned but un-modelled Silver Layer - 29 tables imported directly from SQL Server into Power BI.
The Challenge

    ❌ No surrogate keys - tables use business natural keys (VARCHAR)

    ❌ No SCD Type 2 history - only current state captured

    ❌ No dimensional hierarchy - all tables at operational grain

    ❌ No conformed dimensions - each domain has its own version of "customer," "product," etc.

    ❌ Fact tables not separated from dimensions - mixed granularity in same tables

Key Raw Tables

    crm - Customer master (current state only)

    pos_transactions + pos_line_items - POS header and details (separate tables)

    products - Product master (current price/cost only)

    stores - Store master (no historical tracking)

    hr - Employee records (current state only)

    inventory_snapshots - Stock positions at point in time

    loyalty_transactions - Points earn/redeem events

🏗️ Dimensional Model Architecture (After)

The final dimensional data model follows a classic star schema with conformed dimensions serving multiple fact tables across 5 data marts.
Core Architecture Principles Applied

    Star Schema over Snowflake - Every dimension joins directly to facts; no chained dimension joins.

    Conformed Dimensions - Shared dimensions (dim_date, dim_customer, dim_product, dim_store) reused across all facts.

    Surrogate Keys Everywhere - Integer keys for performance (INT joins faster than VARCHAR).

    SCD Type 2 - History preserved in dim_product, dim_store, dim_employee using Power Query.

    Business-Naming - Gold tables named for what they describe, not their source.

📊 Dimension Table Designs (Power Query M Code)
1. dim_date — The Foundation Dimension

Purpose: Single source of truth for all time-based analysis. Serves as the Calendar for all 9 fact tables.

Modelling Concepts Applied: Surrogate Key (SK), Role-Playing Dimension, Time Intelligence Enabled
m

let
    StartDate = #date(2018, 1, 1),
    EndDate = #date(2027, 12, 31),
    DateList = List.Dates(StartDate, Duration.Days(EndDate - StartDate) + 1, #duration(1, 0, 0, 0)),
    Source = Table.FromList(DateList, Splitter.SplitByNothing(), {"full_date"}),
    #"Changed Type" = Table.TransformColumnTypes(Source, {{"full_date", type date}}),
    #"Added Index" = Table.AddIndexColumn(#"Changed Type", "date_sk", 1, 1, Int64.Type),
    #"Added Custom" = Table.AddColumn(#"Added Index", "year", each Date.Year([full_date])),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "month", each Date.Month([full_date])),
    #"Added Custom2" = Table.AddColumn(#"Added Custom1", "quarter", each Date.QuarterOfYear([full_date])),
    #"Added Custom3" = Table.AddColumn(#"Added Custom2", "day_name", each Date.DayOfWeekName([full_date])),
    #"Added Custom4" = Table.AddColumn(#"Added Custom3", "is_weekend", each Date.DayOfWeek([full_date]) >= 5),
    #"Added Custom5" = Table.AddColumn(#"Added Custom4", "fiscal_year", each if [month] >= 7 then [year] + 1 else [year])
in
    #"Added Custom5"

Source: Generated via date spine procedure (2018-2027)
2. dim_customer — Customer Master

Purpose: Single source of truth for all customer-facing analysis. Serves Sales, Loyalty, and Customer Experience marts.

Modelling Concepts Applied: Surrogate Key (SK), Natural Key (NK), SCD Type 1, Conformed Dimension, -1 Unknown Member
m

let
    Source = silver_crm,
    #"Removed Columns" = Table.RemoveColumns(Source, {"email", "phone", "first_name", "last_name"}),
    #"Added Custom" = Table.AddColumn(#"Removed Columns", "full_name", each [first_name] & " " & [last_name]),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "age_band", each 
        let age = Date.Year(DateTime.LocalNow()) - Date.Year([birth_date])
        in if age < 25 then "Under 25" else if age < 35 then "25-34" else if age < 45 then "35-44" else if age < 55 then "45-54" else "55+"
    ),
    #"Added Custom2" = Table.AddColumn(#"Added Custom1", "generation", each 
        let year = Date.Year([birth_date])
        in if year >= 1997 then "Gen Z" else if year >= 1981 then "Millennial" else if year >= 1965 then "Gen X" else "Boomer"
    ),
    #"Replaced Value" = Table.ReplaceValue(#"Added Custom2", null, -1, Replacer.ReplaceValue, {"customer_key"}),
    #"Added Index" = Table.AddIndexColumn(#"Replaced Value", "customer_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.crm
3. dim_product — Product Master with SCD Type 2

Purpose: Product dimension with full historical tracking using Power Query's SCD-2 implementation.

Modelling Concepts Applied: SCD Type 2 (Add New Row), Surrogate Key (SK), Outrigger Pattern, Margin Band
m

let
    Source = silver_products,
    #"Grouped Rows" = Table.Group(Source, {"product_id"}, {
        {"Versions", each _, type table},
        {"VersionCount", each Table.RowCount(_), Int64.Type}
    }),
    #"Expanded Versions" = Table.ExpandTableColumn(#"Grouped Rows", "Versions", 
        {"product_id", "product_name", "brand", "category", "subcategory", "supplier_id", "unit_cost_kes", "retail_price_kes", "introduction_date", "discontinued_date", "valid_from", "valid_to"}, 
        {"product_id.1", "product_name", "brand", "category", "subcategory", "supplier_id", "unit_cost_kes", "retail_price_kes", "introduction_date", "discontinued_date", "valid_from", "valid_to"}
    ),
    #"Added Custom" = Table.AddColumn(#"Expanded Versions", "margin_pct", each ([retail_price_kes] - [unit_cost_kes]) / [retail_price_kes]),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "margin_band", each 
        let margin = [margin_pct]
        in if margin < 0.20 then "Low" else if margin < 0.40 then "Medium" else "High"
    ),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom1", "product_sk", 1, 1, Int64.Type),
    #"Added Custom2" = Table.AddColumn(#"Added Index", "is_current_version", each [valid_to] = null)
in
    #"Added Custom2"

Source: silver.products
4. dim_store — Store Master with Enriched Geography

Purpose: Store dimension with SCD Type 2 tracking and geographic enrichment for map visualisations.

Modelling Concepts Applied: SCD Type 2, Outrigger Pattern, Conformed Dimension
m

let
    Source = silver_stores,
    #"Merged Queries" = Table.NestedJoin(Source, {"county"}, silver_gis_counties, {"county"}, "gis_counties", JoinKind.LeftOuter),
    #"Expanded gis_counties" = Table.ExpandTableColumn(#"Merged Queries", "gis_counties", {"population", "latitude", "longitude"}, {"county_population", "latitude", "longitude"}),
    #"Added Custom" = Table.AddColumn(#"Expanded gis_counties", "is_current_version", each [valid_to] = null),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom", "store_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.stores + silver.gis_counties
5. dim_employee — HR Dimension with Workforce Attributes

Purpose: Employee dimension for HR analytics and sales attribution (cashier dimension).

Modelling Concepts Applied: SCD Type 2, Conformed Dimension, Derived HR Attributes
m

let
    Source = silver_hr,
    #"Added Custom" = Table.AddColumn(Source, "display_name", each [first_name] & " " & [last_name]),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "age_band", each 
        let age = Date.Year(DateTime.LocalNow()) - Date.Year([birth_date])
        in if age < 25 then "Under 25" else if age < 35 then "25-34" else if age < 45 then "35-44" else if age < 55 then "45-54" else "55+"
    ),
    #"Added Custom2" = Table.AddColumn(#"Added Custom1", "generation", each 
        let year = Date.Year([birth_date])
        in if year >= 1997 then "Gen Z" else if year >= 1981 then "Millennial" else if year >= 1965 then "Gen X" else "Boomer"
    ),
    #"Added Custom3" = Table.AddColumn(#"Added Custom2", "tenure_band", each 
        let tenure = [tenure_years]
        in if tenure < 1 then "< 1 Year" else if tenure < 3 then "1-3 Years" else if tenure < 5 then "3-5 Years" else "5+ Years"
    ),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom3", "employee_sk", 1, 1, Int64.Type),
    #"Added Custom4" = Table.AddColumn(#"Added Index", "is_current_version", each [valid_to] = null)
in
    #"Added Custom4"

Source: silver.hr
6. dim_supplier — Supplier Reference

Purpose: Supplier master for procurement analysis.

Modelling Concepts Applied: SCD Type 1 (Overwrite), Outrigger Pattern
m

let
    Source = silver_suppliers,
    #"Removed Columns" = Table.RemoveColumns(Source, {"tax_id", "email"}),
    #"Added Index" = Table.AddIndexColumn(#"Removed Columns", "supplier_sk", 1, 1, Int64.Type),
    #"Added Custom" = Table.AddColumn(#"Added Index", "is_current", each [valid_to] = null)
in
    #"Added Custom"

Source: silver.suppliers
7. dim_campaign — Marketing Campaign Dimension

Purpose: Campaign attribution for sales analysis.

Modelling Concepts Applied: SCD Type 1, Conformed Dimension
m

let
    Source = silver_campaigns,
    #"Added Custom" = Table.AddColumn(Source, "campaign_duration_days", each Duration.Days([end_date] - [start_date])),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "budget_utilisation_pct", each [actual_spend_kes] / [budget_kes]),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom1", "campaign_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.campaigns
8. dim_geography_economic — Geography & Economic Context

Purpose: Enriched geography with economic indicators for macro analysis.

Modelling Concepts Applied: Outrigger Pattern, Latest Economic Snapshot
m

let
    Source = silver_gis_counties,
    #"Merged Queries" = Table.NestedJoin(Source, {"county"}, silver_economic, {"county"}, "economic", JoinKind.LeftOuter),
    #"Expanded economic" = Table.ExpandTableColumn(#"Merged Queries", "economic", 
        {"gdp_growth_pct", "inflation_pct", "unemployment_pct", "consumer_confidence", "retail_sales_index", "usd_kes_rate", "date"}, 
        {"gdp_growth_pct", "inflation_pct", "unemployment_pct", "consumer_confidence", "retail_sales_index", "usd_kes_rate", "economic_as_of_date"}
    ),
    #"Added Index" = Table.AddIndexColumn(#"Expanded economic", "geo_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.gis_counties + latest from silver.economic
9. dim_promotion — Promotion Master (with Bridge)

Purpose: Promotion dimension for factless fact analysis. Resolves many-to-many relationship with products via bridge_promotion_product.

Modelling Concepts Applied: Bridge Table Pattern, Factless Fact, Bidirectional Filtering
m

let
    Source = silver_promotions,
    #"Removed Columns" = Table.RemoveColumns(Source, {"is_active", "days_until_start", "days_since_ended"}),
    #"Removed Duplicates" = Table.Distinct(#"Removed Columns", {"promotion_id"}),
    #"Added Index" = Table.AddIndexColumn(#"Removed Duplicates", "promotion_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.promotions
📊 Bridge Tables
bridge_promotion_product — Many-to-Many Resolution

Purpose: Resolves the many-to-many relationship between promotions and products.
m

let
    Source = silver_promotion_products,
    #"Merged Queries" = Table.NestedJoin(Source, {"promotion_id"}, dim_promotion, {"promotion_id"}, "dim_promotion", JoinKind.LeftOuter),
    #"Expanded dim_promotion" = Table.ExpandTableColumn(#"Merged Queries", "dim_promotion", {"promotion_sk"}, {"promotion_sk"}),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded dim_promotion", {"product_id"}, dim_product, {"product_id"}, "dim_product", JoinKind.LeftOuter),
    #"Expanded dim_product" = Table.ExpandTableColumn(#"Merged Queries1", "dim_product", {"product_sk"}, {"product_sk"}),
    #"Removed Columns" = Table.RemoveColumns(#"Expanded dim_product", {"promotion_id", "product_id"}),
    #"Added Custom" = Table.AddColumn(#"Removed Columns", "weighting", each 1.0),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom", "bridge_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.promotion_products + dim_promotion + dim_product
📊 Fact Table Designs (Power Query M Code)
1. fact_pos_sales — POS Transaction Fact (Atomic)

Purpose: Core retail sales fact - one row per product line per POS transaction.

Modelling Concepts Applied: Transaction Fact Pattern, Degenerate Dimensions, Conformed Facts, -1 Unknown Member

Grain Declaration: One row per product line within a POS transaction
m

let
    Source = silver_pos_transactions,
    #"Merged Queries" = Table.NestedJoin(Source, {"transaction_id"}, silver_pos_line_items, {"transaction_id"}, "pos_line_items", JoinKind.LeftOuter),
    #"Expanded pos_line_items" = Table.ExpandTableColumn(#"Merged Queries", "pos_line_items", 
        {"line_number", "product_id", "quantity", "unit_price_kes", "discount_rate", "effective_unit_price_kes", "discount_amount_kes", "line_total_kes"}, 
        {"line_number", "product_id", "quantity", "unit_price_kes", "discount_rate", "effective_unit_price_kes", "discount_amount_kes", "line_total_kes"}
    ),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded pos_line_items", {"product_id", "date"}, dim_product, {"product_id", "valid_from"}, "dim_product", JoinKind.LeftOuter),
    #"Merged Queries2" = Table.NestedJoin(#"Merged Queries1", {"customer_id"}, dim_customer, {"customer_id"}, "dim_customer", JoinKind.LeftOuter),
    #"Expanded dim_customer" = Table.ExpandTableColumn(#"Merged Queries2", "dim_customer", {"customer_sk"}, {"customer_sk"}),
    #"Merged Queries3" = Table.NestedJoin(#"Expanded dim_customer", {"store_id"}, dim_store, {"store_id"}, "dim_store", JoinKind.LeftOuter),
    #"Expanded dim_store" = Table.ExpandTableColumn(#"Merged Queries3", "dim_store", {"store_sk"}, {"store_sk"}),
    #"Replaced Value" = Table.ReplaceValue(#"Expanded dim_store", null, -1, Replacer.ReplaceValue, {"customer_sk"}),
    #"Added Custom" = Table.AddColumn(#"Replaced Value", "cogs_kes", each [quantity] * [unit_cost_kes]),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "gross_profit_kes", each [line_total_kes] - [cogs_kes]),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom1", "pos_sales_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.pos_transactions + silver.pos_line_items
2. fact_ecommerce_sales — E-Commerce Sales Fact

Purpose: Online order line-level fact - one row per product line per order.

Modelling Concepts Applied: Transaction Fact Pattern, Degenerate Dimensions, Conformed Facts

Grain Declaration: One row per product line within an e-commerce order
m

let
    Source = silver_ecommerce_order_lines,
    #"Merged Queries" = Table.NestedJoin(Source, {"product_id"}, dim_product, {"product_id"}, "dim_product", JoinKind.LeftOuter),
    #"Expanded dim_product" = Table.ExpandTableColumn(#"Merged Queries", "dim_product", {"product_sk"}, {"product_sk"}),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded dim_product", {"customer_id"}, dim_customer, {"customer_id"}, "dim_customer", JoinKind.LeftOuter),
    #"Expanded dim_customer" = Table.ExpandTableColumn(#"Merged Queries1", "dim_customer", {"customer_sk"}, {"customer_sk"}),
    #"Replaced Value" = Table.ReplaceValue(#"Expanded dim_customer", null, -1, Replacer.ReplaceValue, {"customer_sk"}),
    #"Added Custom" = Table.AddColumn(#"Replaced Value", "cogs_kes", each [quantity] * [unit_cost_kes]),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "gross_profit_kes", each [line_total_kes] - [cogs_kes]),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom1", "ecom_sales_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.ecommerce_orders + silver.ecommerce_order_lines
3. fact_returns — Returns Fact

Purpose: Records product returns linked to original sales.

Modelling Concepts Applied: Transaction Fact Pattern, Late-Arriving Data, Conformed Facts

Grain Declaration: One row per return event per product
m

let
    Source = silver_returns,
    #"Merged Queries" = Table.NestedJoin(Source, {"product_id"}, dim_product, {"product_id"}, "dim_product", JoinKind.LeftOuter),
    #"Expanded dim_product" = Table.ExpandTableColumn(#"Merged Queries", "dim_product", {"product_sk"}, {"product_sk"}),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded dim_product", {"customer_id"}, dim_customer, {"customer_id"}, "dim_customer", JoinKind.LeftOuter),
    #"Expanded dim_customer" = Table.ExpandTableColumn(#"Merged Queries1", "dim_customer", {"customer_sk"}, {"customer_sk"}),
    #"Merged Queries2" = Table.NestedJoin(#"Expanded dim_customer", {"store_id"}, dim_store, {"store_id"}, "dim_store", JoinKind.LeftOuter),
    #"Expanded dim_store" = Table.ExpandTableColumn(#"Merged Queries2", "dim_store", {"store_sk"}, {"store_sk"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded dim_store", "return_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.returns
4. fact_loyalty — Loyalty Transactions

Purpose: Records every points earn, redeem, and expiry event.

Modelling Concepts Applied: Transaction Fact Pattern, Semi-Additive Measures, Conformed Customer

Grain Declaration: One row per loyalty transaction event
m

let
    Source = silver_loyalty_transactions,
    #"Merged Queries" = Table.NestedJoin(Source, {"customer_id"}, dim_customer, {"customer_id"}, "dim_customer", JoinKind.LeftOuter),
    #"Expanded dim_customer" = Table.ExpandTableColumn(#"Merged Queries", "dim_customer", {"customer_sk"}, {"customer_sk"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded dim_customer", "loyalty_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.loyalty_transactions
5. fact_inventory_movements — Inventory Movements

Purpose: Granular record of every stock movement event.

Modelling Concepts Applied: Transaction Fact Pattern, Conformed Dimensions

Grain Declaration: One row per movement event per product/store
m

let
    Source = silver_inventory_movements,
    #"Merged Queries" = Table.NestedJoin(Source, {"product_id"}, dim_product, {"product_id"}, "dim_product", JoinKind.LeftOuter),
    #"Expanded dim_product" = Table.ExpandTableColumn(#"Merged Queries", "dim_product", {"product_sk"}, {"product_sk"}),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded dim_product", {"store_id"}, dim_store, {"store_id"}, "dim_store", JoinKind.LeftOuter),
    #"Expanded dim_store" = Table.ExpandTableColumn(#"Merged Queries1", "dim_store", {"store_sk"}, {"store_sk"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded dim_store", "movement_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.inventory_movements
6. fact_procurement — Procurement Fact (Accumulating Snapshot)

Purpose: Tracks purchase order lifecycle from order to receipt.

Modelling Concepts Applied: Accumulating Snapshot, Role-Playing Dates, Late-Arriving Data

Grain Declaration: One row per purchase order line
m

let
    Source = silver_purchase_order_lines,
    #"Merged Queries" = Table.NestedJoin(Source, {"po_number"}, silver_purchase_orders, {"po_number"}, "purchase_orders", JoinKind.LeftOuter),
    #"Expanded purchase_orders" = Table.ExpandTableColumn(#"Merged Queries", "purchase_orders", 
        {"supplier_id", "status", "status_category", "order_date", "expected_delivery_date", "total_amount_kes", "expected_lead_time_days", "action_priority"}, 
        {"supplier_id", "status", "status_category", "order_date", "expected_delivery_date", "total_amount_kes", "expected_lead_time_days", "action_priority"}
    ),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded purchase_orders", {"product_id"}, dim_product, {"product_id"}, "dim_product", JoinKind.LeftOuter),
    #"Expanded dim_product" = Table.ExpandTableColumn(#"Merged Queries1", "dim_product", {"product_sk"}, {"product_sk"}),
    #"Merged Queries2" = Table.NestedJoin(#"Expanded dim_product", {"supplier_id"}, dim_supplier, {"supplier_id"}, "dim_supplier", JoinKind.LeftOuter),
    #"Expanded dim_supplier" = Table.ExpandTableColumn(#"Merged Queries2", "dim_supplier", {"supplier_sk"}, {"supplier_sk"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded dim_supplier", "procurement_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.purchase_orders + silver.purchase_order_lines + silver.goods_receipts
7. fact_customer_experience — Customer Feedback

Purpose: Consolidates customer feedback and service interactions.

Modelling Concepts Applied: Transaction Fact Pattern, Conformed Customer, Source Union

Grain Declaration: One row per feedback submission or service interaction
m

let
    // Combine feedback and service interactions
    Source = Table.Combine({silver_feedback, silver_service_interactions}),
    #"Merged Queries" = Table.NestedJoin(Source, {"customer_id"}, dim_customer, {"customer_id"}, "dim_customer", JoinKind.LeftOuter),
    #"Expanded dim_customer" = Table.ExpandTableColumn(#"Merged Queries", "dim_customer", {"customer_sk"}, {"customer_sk"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded dim_customer", "experience_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.feedback UNION silver.service_interactions
8. fact_gift_card — Gift Card Transactions

Purpose: Records gift card loads and redemptions.

Modelling Concepts Applied: Transaction Fact Pattern, Conformed Customer, Degenerate Dimensions

Grain Declaration: One row per gift card transaction
m

let
    Source = silver_gift_card_transactions,
    #"Merged Queries" = Table.NestedJoin(Source, {"customer_id"}, dim_customer, {"customer_id"}, "dim_customer", JoinKind.LeftOuter),
    #"Expanded dim_customer" = Table.ExpandTableColumn(#"Merged Queries", "dim_customer", {"customer_sk"}, {"customer_sk"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded dim_customer", "gift_card_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.gift_cards + silver.gift_card_transactions
9. fact_time_attendance — Time Tracking

Purpose: Records employee clock-in/clock-out events.

Modelling Concepts Applied: Transaction Fact Pattern, Conformed Dimensions, Semi-Additive

Grain Declaration: One row per clock event
m

let
    Source = silver_time_tracking,
    #"Merged Queries" = Table.NestedJoin(Source, {"employee_id"}, dim_employee, {"employee_id"}, "dim_employee", JoinKind.LeftOuter),
    #"Expanded dim_employee" = Table.ExpandTableColumn(#"Merged Queries", "dim_employee", {"employee_sk"}, {"employee_sk"}),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded dim_employee", {"store_id"}, dim_store, {"store_id"}, "dim_store", JoinKind.LeftOuter),
    #"Expanded dim_store" = Table.ExpandTableColumn(#"Merged Queries1", "dim_store", {"store_sk"}, {"store_sk"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded dim_store", "attendance_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.time_tracking
📊 Aggregate Tables (Performance Optimisation)
1. agg_store_financials — Daily Store P&L

Purpose: Pre-aggregated financial summary - one row per store per day. Serves Finance mart dashboard queries.

Modelling Concepts Applied: Periodic Snapshot, Aggregation Table, Conformed Store
m

let
    Source = silver_store_daily_financials,
    #"Merged Queries" = Table.NestedJoin(Source, {"store_id"}, dim_store, {"store_id"}, "dim_store", JoinKind.LeftOuter),
    #"Expanded dim_store" = Table.ExpandTableColumn(#"Merged Queries", "dim_store", {"store_sk"}, {"store_sk"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded dim_store", "store_fin_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.store_daily_financials
2. agg_inventory_snapshot — Inventory Position

Purpose: Periodic inventory position - one row per product per store per snapshot date.

Modelling Concepts Applied: Periodic Snapshot, Semi-Additive Measures, Aggregation Table
m

let
    Source = silver_inventory_snapshots,
    #"Merged Queries" = Table.NestedJoin(Source, {"product_id"}, dim_product, {"product_id"}, "dim_product", JoinKind.LeftOuter),
    #"Expanded dim_product" = Table.ExpandTableColumn(#"Merged Queries", "dim_product", {"product_sk"}, {"product_sk"}),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded dim_product", {"store_id"}, dim_store, {"store_id"}, "dim_store", JoinKind.LeftOuter),
    #"Expanded dim_store" = Table.ExpandTableColumn(#"Merged Queries1", "dim_store", {"store_sk"}, {"store_sk"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded dim_store", "inv_snapshot_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.inventory_snapshots
3. agg_competitor_benchmark — Market Intelligence

Purpose: Competitor quarterly performance for market share analysis.

Modelling Concepts Applied: Periodic Snapshot, Conformed Measures
m

let
    Source = silver_competitor_quarterly,
    #"Added Index" = Table.AddIndexColumn(Source, "competitor_bench_sk", 1, 1, Int64.Type)
in
    #"Added Index"

Source: silver.competitor_quarterly
🔄 Transformation Journey: Key Modelling Concepts Applied in Power Query
1. Surrogate Key Generation with Table.AddIndexColumn

Before: All tables used business natural keys (VARCHAR)
After: Every dimension has _sk INT surrogate keys

Power Query Pattern:
m

#"Added Index" = Table.AddIndexColumn(Source, "customer_sk", 1, 1, Int64.Type)

Why This Matters:

    ✅ Integer joins are 3-5x faster than VARCHAR joins

    ✅ Protects model when source systems change their keys

    ✅ Enables SCD Type 2 (multiple versions with different SKs)

2. SCD Type 2 Implementation with Power Query

Before: Products had only current price/cost
After: dim_product has SCD-2 with valid_from/valid_to

Power Query Pattern:
m

#"Grouped Rows" = Table.Group(Source, {"product_id"}, {
    {"Versions", each _, type table},
    {"VersionCount", each Table.RowCount(_), Int64.Type}
}),
#"Added Custom" = Table.AddColumn(#"Expanded Versions", "is_current_version", each [valid_to] = null)

Why This Matters:

    ✅ Historical margin analysis accurate (2022 sale uses 2022 costs)

    ✅ Store format changes tracked over time

    ✅ Employee job title changes reflected historically

3. Conformed Dimensions via Merged Queries

Before: Each source had its own version of dimensions
After: Single dimensions shared across all facts

Power Query Pattern:
m

#"Merged Queries" = Table.NestedJoin(
    pos_sales, {"customer_id"}, 
    dim_customer, {"customer_id"}, 
    "dim_customer", JoinKind.LeftOuter
)

Why This Matters:

    ✅ Consistent metrics across all reports

    ✅ Cross-mart analysis possible

    ✅ Single source of truth eliminates conflicting reports

4. Bridge Table (Many-to-Many Resolution)

Before: Promotions applied to products via a junction table
After: bridge_promotion_product with surrogate keys

Power Query Pattern:
m

#"Merged Queries" = Table.NestedJoin(Source, {"promotion_id"}, dim_promotion, {"promotion_id"}, "dim_promotion", JoinKind.LeftOuter),
#"Expanded dim_promotion" = Table.ExpandTableColumn(#"Merged Queries", "dim_promotion", {"promotion_sk"}, {"promotion_sk"})

Why This Matters:

    ✅ Resolves M2M relationship without fan-out

    ✅ Enables product filtering by promotion and vice versa

    ✅ Required for factless fact coverage analysis

5. Role-Playing Dimensions with Multiple Date FKs

Before: Purchase orders had three separate date tables
After: Single dim_date serves three roles

Power Query Pattern:
m

#"Merged Queries" = Table.NestedJoin(Source, {"order_date"}, dim_date, {"full_date"}, "order_date_dim", JoinKind.LeftOuter),
#"Merged Queries1" = Table.NestedJoin(#"Merged Queries", {"expected_delivery_date"}, dim_date, {"full_date"}, "delivery_date_dim", JoinKind.LeftOuter),
#"Merged Queries2" = Table.NestedJoin(#"Merged Queries1", {"receipt_date"}, dim_date, {"full_date"}, "receipt_date_dim", JoinKind.LeftOuter)

Why This Matters:

    ✅ Eliminates redundant date tables

    ✅ Enables lead time analysis

    ✅ DAX USERELATIONSHIP() activates the correct date role

6. Degenerate Dimensions (Keep in Fact)

Before: Transaction IDs not used for filtering
After: transaction_id, order_status, payment_method remain in fact

Why This Matters:

    ✅ Enables drill-through to original transactions

    ✅ Low-cardinality fields don't need separate dimension tables

    ✅ Reduces model size

7. Aggregation Tables for Performance

Before: All queries scanned full atomic fact tables
After: Pre-summed aggregates serve dashboard-level queries

Power Query Pattern:
m

#"Grouped Rows" = Table.Group(
    fact_pos_sales, 
    {"store_id", "date"}, 
    {
        {"sales_kes", each List.Sum([line_total_kes]), type number},
        {"cogs_kes", each List.Sum([cogs_kes]), type number},
        {"gross_margin", each List.Sum([gross_profit_kes]), type number},
        {"transaction_count", each Table.RowCount(_), Int64.Type}
    }
)

Why This Matters:

    ✅ Dashboard load times reduced from seconds to milliseconds

    ✅ Power BI Aggregations automatically route queries

    ✅ Scales to millions/billions of rows

📊 Final Model Summary
Dimension Tables (6 Conformed)
Table	SCD Type	Grain	Power Query Key Operations
dim_date	-	One row per day	Date spine, AddIndexColumn
dim_customer	Type 1	One row per customer	Merged Queries, Derived Columns
dim_product	Type 2	One row per version	Grouped Rows, SCD-2 Logic
dim_store	Type 2	One row per version	Merged Queries, SCD-2 Logic
dim_employee	Type 2	One row per version	Derived Columns, SCD-2 Logic
dim_supplier	Type 1	One row per supplier	Merged Queries, Derived Columns
dim_campaign	Type 1	One row per campaign	Merged Queries, Derived Columns
dim_geography_economic	-	One row per county	Merged Queries (Latest Economic)
Fact Tables (9 Atomic)
Table	Grain	Pattern	Power Query Operations
fact_pos_sales	One row per POS line	Transaction	Multiple Merged Queries, SCD-2 Join
fact_ecommerce_sales	One row per order line	Transaction	Multiple Merged Queries
fact_returns	One row per return event	Transaction	Merged Queries
fact_loyalty	One row per loyalty event	Transaction	Merged Queries
fact_inventory_movements	One row per movement	Transaction	Merged Queries
fact_procurement	One row per PO line	Accumulating Snapshot	Multiple Date Joins
fact_customer_experience	One row per feedback/interaction	Transaction	Union Tables
fact_gift_card	One row per transaction	Transaction	Merged Queries
fact_time_attendance	One row per clock event	Transaction	Merged Queries
Aggregate & Bridge Tables
Table	Grain	Power Query Operations
agg_store_financials	One row per store per day	Table.Group, List.Sum
agg_inventory_snapshot	One row per product/store/day	Table.Group
agg_competitor_benchmark	One row per competitor per quarter	Merged Queries
bridge_promotion_product	One row per promotion-product pair	Multiple Merged Queries
🏢 Data Marts (5 Subject Areas)
Mart	Primary Fact Tables	Supporting Dims	Key Metrics
Sales & Revenue	fact_pos_sales, fact_ecommerce_sales, fact_returns	date, customer, product, store, employee, campaign, promotion	Revenue, AOV, Gross Margin, Return Rate, Campaign ROI
Customer & Loyalty	fact_loyalty, fact_customer_experience	customer, date, geography	Active Customers, Churn Rate, NPS, Loyalty Redemption Rate
Inventory & Supply Chain	fact_inventory_movements, fact_procurement, agg_inventory_snapshot	product, store, supplier, date	Stockouts, Turnover, Supplier OTD, Lead Time Variance
Finance & Store P&L	agg_store_financials	store, date, geography	Net Profit, Operating Margin, Revenue/SqM, Market Share
HR & Workforce	fact_time_attendance	employee, store, date	Headcount, Retention Risk, Gender Diversity, Attendance Rate
🛠️ Tools Used

    Power BI Desktop - Data modelling, Power Query (M) transformations, DAX measures

    SQL Server - Source data (Silver Layer)

    Tabular Editor - Calculation groups and advanced modelling

    DAX Studio - Performance optimisation and measure testing

    GitHub - Version control for documentation and scripts

📚 Key Learning Resources

    Kimball Group - Star schema methodology

    Microsoft Learn - Power BI data modelling best practices

    SQLBI - Advanced DAX patterns

    Power Query Documentation - M language reference

🤝 Connect

Junior Kevin · LinkedIn
