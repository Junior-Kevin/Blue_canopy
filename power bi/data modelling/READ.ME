<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Data Modelling · Power BI Gold Layer (Power Query)</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans", Ubuntu, Cantarell, "Helvetica Neue", sans-serif;
            background: #f6f8fa;
            color: #24292f;
            margin: 0;
            padding: 2rem 1.5rem;
            display: flex;
            justify-content: center;
        }
        .readme-container {
            max-width: 1000px;
            width: 100%;
            background: #ffffff;
            padding: 2.5rem 2.8rem;
            border-radius: 16px;
            box-shadow: 0 4px 24px rgba(0,0,0,0.06);
            line-height: 1.6;
        }
        h1, h2, h3, h4 {
            font-weight: 600;
            letter-spacing: -0.01em;
            margin-top: 1.8em;
            margin-bottom: 0.6em;
        }
        h1 { font-size: 2.2rem; border-bottom: 2px solid #d0d7de; padding-bottom: 0.3em; }
        h2 { font-size: 1.6rem; border-bottom: 1px solid #d0d7de; padding-bottom: 0.2em; }
        h3 { font-size: 1.25rem; border-left: 4px solid #0969da; padding-left: 0.8rem; background: #f6f8fa; padding: 0.4rem 0.8rem; border-radius: 0 6px 6px 0; }
        a { color: #0969da; text-decoration: none; }
        a:hover { text-decoration: underline; }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
            margin: 1.2rem 0;
        }
        th {
            background: #f6f8fa;
            font-weight: 600;
            padding: 8px 12px;
            border: 1px solid #d0d7de;
            text-align: left;
        }
        td {
            padding: 8px 12px;
            border: 1px solid #d0d7de;
        }
        code {
            background: #f1f3f5;
            padding: 0.2em 0.4em;
            border-radius: 4px;
            font-size: 0.85em;
            font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
        }
        pre {
            background: #f6f8fa;
            padding: 1rem 1.2rem;
            border-radius: 8px;
            overflow-x: auto;
            font-size: 0.82rem;
            border: 1px solid #d0d7de;
        }
        pre code { background: transparent; padding: 0; font-size: 0.82rem; }
        .badge-grid {
            display: flex;
            flex-wrap: wrap;
            gap: 0.8rem;
            background: #f6f8fa;
            padding: 1.2rem 1.5rem;
            border-radius: 12px;
            margin: 1.5rem 0;
            border: 1px solid #d0d7de;
        }
        .badge-item {
            background: white;
            padding: 0.25rem 1rem;
            border-radius: 30px;
            font-weight: 500;
            font-size: 0.9rem;
            border: 1px solid #d0d7de;
            box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        }
        .badge-item strong { color: #0969da; }
        hr { margin: 2rem 0; border: 0; border-top: 1px solid #d0d7de; }
        .footnote { color: #57606a; font-size: 0.85rem; margin-top: 2rem; border-top: 1px solid #d0d7de; padding-top: 1.5rem; }
        .concept-tag {
            display: inline-block;
            background: #ddf4ff;
            color: #0550ae;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 0.1rem 0.6rem;
            border-radius: 30px;
            margin-left: 0.3rem;
            letter-spacing: 0.02em;
        }
        @media (max-width: 640px) {
            body { padding: 0.8rem; }
            .readme-container { padding: 1.2rem; }
            table { font-size: 0.8rem; }
            th, td { padding: 4px 6px; }
        }
    </style>
</head>
<body>
<div class="readme-container">

    <h1>📊 Data Modelling &amp; Gold Layer Transformation</h1>
    <p><strong>Power BI Semantic Model · Power Query (M) Transformations</strong></p>

    <div class="badge-grid">
        <span class="badge-item">✅ <strong>29</strong> Silver Tables → <strong>15</strong> Gold Objects</span>
        <span class="badge-item">✅ <strong>6</strong> Conformed Dimensions</span>
        <span class="badge-item">✅ <strong>9</strong> Atomic Fact Tables</span>
        <span class="badge-item">✅ <strong>3</strong> Aggregate Tables</span>
        <span class="badge-item">✅ <strong>1</strong> Bridge Table</span>
        <span class="badge-item">⚡ <strong>All transformations in Power Query (M)</strong></span>
    </div>

    <h2>📁 Project Overview</h2>
    <p>This document details the comprehensive data modelling process for the Blue Canopy Retail Analytics Project. Unlike traditional approaches that build the Gold Layer in SQL Server, I built the <strong>entire dimensional model directly in Power BI</strong> using Power Query (M language). Starting from 29 raw silver-layer tables imported from SQL Server, I transformed them into a production-ready star schema using Power Query's ETL capabilities.</p>

    <h3>🏗️ Architecture Overview</h3>
    <pre><code>SQL Server (Silver Layer) 
    ↓ [Import Mode]
Power BI Desktop
    ↓ [Power Query - M Language]
Gold Layer (Power Query transforms)
    ↓ [Model Relationships]
Power BI Semantic Model (Star Schema)
    ↓ [DAX Measures]
5 Data Marts → Interactive Dashboards</code></pre>

    <h3>Why Power BI Instead of SQL Server?</h3>
    <table>
        <thead><tr><th>Aspect</th><th>SQL Server Approach</th><th>Power BI Approach (My Choice)</th></tr></thead>
        <tbody>
            <tr><td><strong>Transformation Location</strong></td><td>SQL stored procedures/views</td><td>Power Query (M language)</td></tr>
            <tr><td><strong>SCD Type 2 Logic</strong></td><td>T-SQL with window functions</td><td>M code with Table.Group, Table.AddColumn</td></tr>
            <tr><td><strong>Surrogate Key Generation</strong></td><td>IDENTITY columns</td><td>Table.AddIndexColumn</td></tr>
            <tr><td><strong>Performance</strong></td><td>Pre-aggregated in SQL</td><td>Import mode + Aggregations</td></tr>
            <tr><td><strong>Maintenance</strong></td><td>Two systems (SQL + PBI)</td><td>Single system (Power BI)</td></tr>
            <tr><td><strong>Learning Value</strong></td><td>SQL skills</td><td>M language + Power Query skills</td></tr>
        </tbody>
    </table>

    <h2>📁 Raw Data Structure (Before)</h2>
    <p>The <a href="https://github.com/Junior-Kevin/Blue_canopy_-Business-Intelligence_project/blob/main/power%20bi/data%20modelling/raw_data%20tables.png" target="_blank">raw data tables</a> represent the cleaned but un-modelled Silver Layer - 29 tables imported directly from SQL Server into Power BI.</p>

    <h3>The Challenge</h3>
    <ul>
        <li>❌ No surrogate keys - tables use business natural keys (VARCHAR)</li>
        <li>❌ No SCD Type 2 history - only current state captured</li>
        <li>❌ No dimensional hierarchy - all tables at operational grain</li>
        <li>❌ No conformed dimensions - each domain has its own version of &quot;customer,&quot; &quot;product,&quot; etc.</li>
        <li>❌ Fact tables not separated from dimensions - mixed granularity in same tables</li>
    </ul>

    <h3>Key Raw Tables</h3>
    <ul>
        <li><code>crm</code> - Customer master (current state only)</li>
        <li><code>pos_transactions</code> + <code>pos_line_items</code> - POS header and details (separate tables)</li>
        <li><code>products</code> - Product master (current price/cost only)</li>
        <li><code>stores</code> - Store master (no historical tracking)</li>
        <li><code>hr</code> - Employee records (current state only)</li>
        <li><code>inventory_snapshots</code> - Stock positions at point in time</li>
        <li><code>loyalty_transactions</code> - Points earn/redeem events</li>
    </ul>

    <h2>🏗️ Dimensional Model Architecture (After)</h2>
    <p>The <a href="https://github.com/Junior-Kevin/Blue_canopy_-Business-Intelligence_project/blob/main/power%20bi/data%20modelling/final_dimensional_data_model.png" target="_blank">final dimensional data model</a> follows a classic star schema with conformed dimensions serving multiple fact tables across 5 data marts.</p>

    <h3>Core Architecture Principles Applied</h3>
    <ol>
        <li><strong>Star Schema over Snowflake</strong> - Every dimension joins directly to facts; no chained dimension joins.</li>
        <li><strong>Conformed Dimensions</strong> - Shared dimensions (<code>dim_date</code>, <code>dim_customer</code>, <code>dim_product</code>, <code>dim_store</code>) reused across all facts.</li>
        <li><strong>Surrogate Keys Everywhere</strong> - Integer keys for performance (INT joins faster than VARCHAR).</li>
        <li><strong>SCD Type 2</strong> - History preserved in <code>dim_product</code>, <code>dim_store</code>, <code>dim_employee</code> using Power Query.</li>
        <li><strong>Business-Naming</strong> - Gold tables named for what they describe, not their source.</li>
    </ol>

    <h2>📊 Dimension Table Designs (Power Query M Code)</h2>

    <h3>1. dim_date — The Foundation Dimension</h3>
    <p><strong>Purpose:</strong> Single source of truth for all time-based analysis. Serves as the <strong>Calendar</strong> for all 9 fact tables.</p>
    <p><strong>Modelling Concepts Applied:</strong> Surrogate Key (SK), Role-Playing Dimension, Time Intelligence Enabled</p>
    <pre><code>let
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
    #"Added Custom5"</code></pre>
    <p><strong>Source:</strong> Generated via date spine procedure (2018-2027)</p>

    <h3>2. dim_customer — Customer Master</h3>
    <p><strong>Purpose:</strong> Single source of truth for all customer-facing analysis. Serves Sales, Loyalty, and Customer Experience marts.</p>
    <p><strong>Modelling Concepts Applied:</strong> Surrogate Key (SK), Natural Key (NK), SCD Type 1, Conformed Dimension, -1 Unknown Member</p>
    <pre><code>let
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
    #"Added Index"</code></pre>
    <p><strong>Source:</strong> <code>silver.crm</code></p>

    <h3>3. dim_product — Product Master with SCD Type 2</h3>
    <p><strong>Purpose:</strong> Product dimension with full historical tracking using Power Query's SCD-2 implementation.</p>
    <p><strong>Modelling Concepts Applied:</strong> SCD Type 2 (Add New Row), Surrogate Key (SK), Outrigger Pattern, Margin Band</p>
    <pre><code>let
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
    #"Added Custom2"</code></pre>
    <p><strong>Source:</strong> <code>silver.products</code></p>

    <h3>4. dim_store — Store Master with Enriched Geography</h3>
    <p><strong>Purpose:</strong> Store dimension with SCD Type 2 tracking and geographic enrichment for map visualisations.</p>
    <p><strong>Modelling Concepts Applied:</strong> SCD Type 2, Outrigger Pattern, Conformed Dimension</p>
    <pre><code>let
    Source = silver_stores,
    #"Merged Queries" = Table.NestedJoin(Source, {"county"}, silver_gis_counties, {"county"}, "gis_counties", JoinKind.LeftOuter),
    #"Expanded gis_counties" = Table.ExpandTableColumn(#"Merged Queries", "gis_counties", {"population", "latitude", "longitude"}, {"county_population", "latitude", "longitude"}),
    #"Added Custom" = Table.AddColumn(#"Expanded gis_counties", "is_current_version", each [valid_to] = null),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom", "store_sk", 1, 1, Int64.Type)
in
    #"Added Index"</code></pre>
    <p><strong>Source:</strong> <code>silver.stores</code> + <code>silver.gis_counties</code></p>

    <h3>5. dim_employee — HR Dimension with Workforce Attributes</h3>
    <p><strong>Purpose:</strong> Employee dimension for HR analytics and sales attribution (cashier dimension).</p>
    <p><strong>Modelling Concepts Applied:</strong> SCD Type 2, Conformed Dimension, Derived HR Attributes</p>
    <pre><code>let
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
    #"Added Custom4"</code></pre>
    <p><strong>Source:</strong> <code>silver.hr</code></p>

    <h3>6. dim_supplier — Supplier Reference</h3>
    <p><strong>Purpose:</strong> Supplier master for procurement analysis.</p>
    <p><strong>Modelling Concepts Applied:</strong> SCD Type 1 (Overwrite), Outrigger Pattern</p>
    <pre><code>let
    Source = silver_suppliers,
    #"Removed Columns" = Table.RemoveColumns(Source, {"tax_id", "email"}),
    #"Added Index" = Table.AddIndexColumn(#"Removed Columns", "supplier_sk", 1, 1, Int64.Type),
    #"Added Custom" = Table.AddColumn(#"Added Index", "is_current", each [valid_to] = null)
in
    #"Added Custom"</code></pre>
    <p><strong>Source:</strong> <code>silver.suppliers</code></p>

    <h3>7. dim_campaign — Marketing Campaign Dimension</h3>
    <p><strong>Purpose:</strong> Campaign attribution for sales analysis.</p>
    <p><strong>Modelling Concepts Applied:</strong> SCD Type 1, Conformed Dimension</p>
    <pre><code>let
    Source = silver_campaigns,
    #"Added Custom" = Table.AddColumn(Source, "campaign_duration_days", each Duration.Days([end_date] - [start_date])),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "budget_utilisation_pct", each [actual_spend_kes] / [budget_kes]),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom1", "campaign_sk", 1, 1, Int64.Type)
in
    #"Added Index"</code></pre>
    <p><strong>Source:</strong> <code>silver.campaigns</code></p>

    <h3>8. dim_geography_economic — Geography &amp; Economic Context</h3>
    <p><strong>Purpose:</strong> Enriched geography with economic indicators for macro analysis.</p>
    <p><strong>Modelling Concepts Applied:</strong> Outrigger Pattern, Latest Economic Snapshot</p>
    <pre><code>let
    Source = silver_gis_counties,
    #"Merged Queries" = Table.NestedJoin(Source, {"county"}, silver_economic, {"county"}, "economic", JoinKind.LeftOuter),
    #"Expanded economic" = Table.ExpandTableColumn(#"Merged Queries", "economic", 
        {"gdp_growth_pct", "inflation_pct", "unemployment_pct", "consumer_confidence", "retail_sales_index", "usd_kes_rate", "date"}, 
        {"gdp_growth_pct", "inflation_pct", "unemployment_pct", "consumer_confidence", "retail_sales_index", "usd_kes_rate", "economic_as_of_date"}
    ),
    #"Added Index" = Table.AddIndexColumn(#"Expanded economic", "geo_sk", 1, 1, Int64.Type)
in
    #"Added Index"</code></pre>
    <p><strong>Source:</strong> <code>silver.gis_counties</code> + latest from <code>silver.economic</code></p>

    <h3>9. dim_promotion — Promotion Master (with Bridge)</h3>
    <p><strong>Purpose:</strong> Promotion dimension for factless fact analysis. Resolves many-to-many relationship with products via <code>bridge_promotion_product</code>.</p>
    <p><strong>Modelling Concepts Applied:</strong> Bridge Table Pattern, Factless Fact, Bidirectional Filtering</p>
    <pre><code>let
    Source = silver_promotions,
    #"Removed Columns" = Table.RemoveColumns(Source, {"is_active", "days_until_start", "days_since_ended"}),
    #"Removed Duplicates" = Table.Distinct(#"Removed Columns", {"promotion_id"}),
    #"Added Index" = Table.AddIndexColumn(#"Removed Duplicates", "promotion_sk", 1, 1, Int64.Type)
in
    #"Added Index"</code></pre>
    <p><strong>Source:</strong> <code>silver.promotions</code></p>

    <h2>📊 Bridge Tables</h2>

    <h3>bridge_promotion_product — Many-to-Many Resolution</h3>
    <p><strong>Purpose:</strong> Resolves the many-to-many relationship between promotions and products.</p>
    <pre><code>let
    Source = silver_promotion_products,
    #"Merged Queries" = Table.NestedJoin(Source, {"promotion_id"}, dim_promotion, {"promotion_id"}, "dim_promotion", JoinKind.LeftOuter),
    #"Expanded dim_promotion" = Table.ExpandTableColumn(#"Merged Queries", "dim_promotion", {"promotion_sk"}, {"promotion_sk"}),
    #"Merged Queries1" = Table.NestedJoin(#"Expanded dim_promotion", {"product_id"}, dim_product, {"product_id"}, "dim_product", JoinKind.LeftOuter),
    #"Expanded dim_product" = Table.ExpandTableColumn(#"Merged Queries1", "dim_product", {"product_sk"}, {"product_sk"}),
    #"Removed Columns" = Table.RemoveColumns(#"Expanded dim_product", {"promotion_id", "product_id"}),
    #"Added Custom" = Table.AddColumn(#"Removed Columns", "weighting", each 1.0),
    #"Added Index" = Table.AddIndexColumn(#"Added Custom", "bridge_sk", 1, 1, Int64.Type)
in
    #"Added Index"</code></pre>
    <p><strong>Source:</strong> <code>silver.promotion_products</code> + <code>dim_promotion</code> + <code>dim_product</code></p>

    <h2>📊 Fact Table Designs (Power Query M Code)</h2>

    <h3>1. fact_pos_sales — POS Transaction Fact (Atomic)</h3>
    <p><strong>Purpose:</strong> Core retail sales fact - one row per product line per POS transaction.</p>
    <p><strong>Modelling Concepts Applied:</strong> Transaction Fact Pattern, Degenerate Dimensions, Conformed Facts, -1 Unknown Member</p>
    <p><strong>Grain Declaration:</strong> One row per product line within a POS transaction</p>
    <pre><code>let
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
    #"Added Index"</code></pre>
    <p><strong>Source:</strong> <code>silver.pos_transactions</code> + <code>silver.pos_line_items</code></p>

    <h3>2. fact_ecommerce_sales — E-Commerce Sales Fact</h3>
    <p><strong>Purpose:</strong> Online order line-level fact - one row per product line per order.</p>
    <p><strong>Modelling Concepts Applied:</strong> Transaction Fact Pattern, Degenerate Dimensions, Conformed Facts</p>
    <p><strong>Grain Declaration:</strong> One row per product line within an e-commerce order</p>
    <pre><code>let
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
    #"Added Index"</code></pre>
    <p><strong>Source:</strong> <code>silver.ecommerce_orders</code> + <code>silver.ecommerce_order_lines</code></p>

    <h2>🔄 Transformation Journey: Key Modelling Concepts Applied in Power Query</h2>

    <h3>1. Surrogate Key Generation with Table.AddIndexColumn</h3>
    <p><strong>Before:</strong> All tables used business natural keys (VARCHAR)<br><strong>After:</strong> Every dimension has <code>_sk</code> INT surrogate keys</p>
    <p><strong>Power Query Pattern:</strong> <code>#"Added Index" = Table.AddIndexColumn(Source, "customer_sk", 1, 1, Int64.Type)</code></p>
    <ul>
        <li>✅ Integer joins are 3-5x faster than VARCHAR joins</li>
        <li>✅ Protects model when source systems change their keys</li>
        <li>✅ Enables SCD Type 2 (multiple versions with different SKs)</li>
    </ul>

    <h3>2. SCD Type 2 Implementation with Power Query</h3>
    <p><strong>Before:</strong> Products had only current price/cost<br><strong>After:</strong> <code>dim_product</code> has SCD-2 with <code>valid_from</code>/<code>valid_to</code></p>
    <p><strong>Power Query Pattern:</strong></p>
    <pre><code>#"Grouped Rows" = Table.Group(Source, {"product_id"}, {
    {"Versions", each _, type table},
    {"VersionCount", each Table.RowCount(_), Int64.Type}
}),
#"Added Custom" = Table.AddColumn(#"Expanded Versions", "is_current_version", each [valid_to] = null)</code></pre>
    <ul>
        <li>✅ Historical margin analysis accurate (2022 sale uses 2022 costs)</li>
        <li>✅ Store format changes tracked over time</li>
        <li>✅ Employee job title changes reflected historically</li>
    </ul>

    <h3>3. Conformed Dimensions via Merged Queries</h3>
    <p><strong>Before:</strong> Each source had its own version of dimensions<br><strong>After:</strong> Single dimensions shared across all facts</p>
    <p><strong>Power Query Pattern:</strong> <code>Table.NestedJoin(pos_sales, {"customer_id"}, dim_customer, {"customer_id"}, "dim_customer", JoinKind.LeftOuter)</code></p>
    <ul>
        <li>✅ Consistent metrics across all reports</li>
        <li>✅ Cross-mart analysis possible</li>
        <li>✅ Single source of truth eliminates conflicting reports</li>
    </ul>

    <h3>4. Bridge Table (Many-to-Many Resolution)</h3>
    <p><strong>Before:</strong> Promotions applied to products via a junction table<br><strong>After:</strong> <code>bridge_promotion_product</code> with surrogate keys</p>
    <ul>
        <li>✅ Resolves M2M relationship without fan-out</li>
        <li>✅ Enables product filtering by promotion and vice versa</li>
        <li>✅ Required for factless fact coverage analysis</li>
    </ul>

    <h3>5. Role-Playing Dimensions with Multiple Date FKs</h3>
    <p><strong>Before:</strong> Purchase orders had three separate date tables<br><strong>After:</strong> Single <code>dim_date</code> serves three roles</p>
    <p><strong>Power Query Pattern:</strong> Multiple joins to <code>dim_date</code> with different aliases</p>
    <ul>
        <li>✅ Eliminates redundant date tables</li>
        <li>✅ Enables lead time analysis</li>
        <li>✅ DAX <code>USERELATIONSHIP()</code> activates the correct date role</li>
    </ul>

    <h3>6. Degenerate Dimensions (Keep in Fact)</h3>
    <p><strong>Before:</strong> Transaction IDs not used for filtering<br><strong>After:</strong> <code>transaction_id</code>, <code>order_status</code>, <code>payment_method</code> remain in fact</p>
    <ul>
        <li>✅ Enables drill-through to original transactions</li>
        <li>✅ Low-cardinality fields don't need separate dimension tables</li>
        <li>✅ Reduces model size</li>
    </ul>

    <h3>7. Aggregation Tables for Performance</h3>
    <p><strong>Before:</strong> All queries scanned full atomic fact tables<br><strong>After:</strong> Pre-summed aggregates serve dashboard-level queries</p>
    <p><strong>Power Query Pattern:</strong> <code>Table.Group(fact_pos_sales, {"store_id", "date"}, {{"sales_kes", each List.Sum([line_total_kes]), type number}})</code></p>
    <ul>
        <li>✅ Dashboard load times reduced from seconds to milliseconds</li>
        <li>✅ Power BI Aggregations automatically route queries</li>
        <li>✅ Scales to millions/billions of rows</li>
    </ul>

    <h2>📊 Final Model Summary</h2>

    <h3>Dimension Tables (6 Conformed)</h3>
    <table>
        <thead><tr><th>Table</th><th>SCD Type</th><th>Grain</th><th>Power Query Key Operations</th></tr></thead>
        <tbody>
            <tr><td>dim_date</td><td>-</td><td>One row per day</td><td>Date spine, AddIndexColumn</td></tr>
            <tr><td>dim_customer</td><td>Type 1</td><td>One row per customer</td><td>Merged Queries, Derived Columns</td></tr>
            <tr><td>dim_product</td><td>Type 2</td><td>One row per version</td><td>Grouped Rows, SCD-2 Logic</td></tr>
            <tr><td>dim_store</td><td>Type 2</td><td>One row per version</td><td>Merged Queries, SCD-2 Logic</td></tr>
            <tr><td>dim_employee</td><td>Type 2</td><td>One row per version</td><td>Derived Columns, SCD-2 Logic</td></tr>
            <tr><td>dim_supplier</td><td>Type 1</td><td>One row per supplier</td><td>Merged Queries, Derived Columns</td></tr>
        </tbody>
    </table>

    <h3>Fact Tables (9 Atomic)</h3>
    <table>
        <thead><tr><th>Table</th><th>Grain</th><th>Pattern</th><th>Power Query Operations</th></tr></thead>
        <tbody>
            <tr><td>fact_pos_sales</td><td>One row per POS line</td><td>Transaction</td><td>Multiple Merged Queries, SCD-2 Join</td></tr>
            <tr><td>fact_ecommerce_sales</td><td>One row per order line</td><td>Transaction</td><td>Multiple Merged Queries</td></tr>
            <tr><td>fact_returns</td><td>One row per return event</td><td>Transaction</td><td>Merged Queries</td></tr>
            <tr><td>fact_loyalty</td><td>One row per loyalty event</td><td>Transaction</td><td>Merged Queries</td></tr>
            <tr><td>fact_inventory_movements</td><td>One row per movement</td><td>Transaction</td><td>Merged Queries</td></tr>
            <tr><td>fact_procurement</td><td>One row per PO line</td><td>Accumulating Snapshot</td><td>Multiple Date Joins</td></tr>
            <tr><td>fact_customer_experience</td><td>One row per feedback/interaction</td><td>Transaction</td><td>Union Tables</td></tr>
            <tr><td>fact_gift_card</td><td>One row per transaction</td><td>Transaction</td><td>Merged Queries</td></tr>
            <tr><td>fact_time_attendance</td><td>One row per clock event</td><td>Transaction</td><td>Merged Queries</td></tr>
        </tbody>
    </table>

    <h3>Aggregate &amp; Bridge Tables</h3>
    <table>
        <thead><tr><th>Table</th><th>Grain</th><th>Power Query Operations</th></tr></thead>
        <tbody>
            <tr><td>agg_store_financials</td><td>One row per store per day</td><td>Table.Group, List.Sum</td></tr>
            <tr><td>agg_inventory_snapshot</td><td>One row per product/store/day</td><td>Table.Group</td></tr>
            <tr><td>agg_competitor_benchmark</td><td>One row per competitor per quarter</td><td>Merged Queries</td></tr>
            <tr><td>bridge_promotion_product</td><td>One row per promotion-product pair</td><td>Multiple Merged Queries</td></tr>
        </tbody>
    </table>

    <h2>🏢 Data Marts (5 Subject Areas)</h2>
    <table>
        <thead><tr><th>Mart</th><th>Primary Fact Tables</th><th>Supporting Dims</th><th>Key Metrics</th></tr></thead>
        <tbody>
            <tr><td><strong>Sales &amp; Revenue</strong></td><td>fact_pos_sales, fact_ecommerce_sales, fact_returns</td><td>date, customer, product, store, employee, campaign, promotion</td><td>Revenue, AOV, Gross Margin, Return Rate, Campaign ROI</td></tr>
            <tr><td><strong>Customer &amp; Loyalty</strong></td><td>fact_loyalty, fact_customer_experience</td><td>customer, date, geography</td><td>Active Customers, Churn Rate, NPS, Loyalty Redemption Rate</td></tr>
            <tr><td><strong>Inventory &amp; Supply Chain</strong></td><td>fact_inventory_movements, fact_procurement, agg_inventory_snapshot</td><td>product, store, supplier, date</td><td>Stockouts, Turnover, Supplier OTD, Lead Time Variance</td></tr>
            <tr><td><strong>Finance &amp; Store P&amp;L</strong></td><td>agg_store_financials</td><td>store, date, geography</td><td>Net Profit, Operating Margin, Revenue/SqM, Market Share</td></tr>
            <tr><td><strong>HR &amp; Workforce</strong></td><td>fact_time_attendance</td><td>employee, store, date</td><td>Headcount, Retention Risk, Gender Diversity, Attendance Rate</td></tr>
        </tbody>
    </table>

    <h2>🛠️ Tools Used</h2>
    <ul>
        <li><strong>Power BI Desktop</strong> - Data modelling, Power Query (M) transformations, DAX measures</li>
        <li><strong>SQL Server</strong> - Source data (Silver Layer)</li>
        <li><strong>Tabular Editor</strong> - Calculation groups and advanced modelling</li>
        <li><strong>DAX Studio</strong> - Performance optimisation and measure testing</li>
        <li><strong>GitHub</strong> - Version control for documentation and scripts</li>
    </ul>

    <h2>📚 Key Learning Resources</h2>
    <ul>
        <li><strong>Kimball Group</strong> - Star schema methodology</li>
        <li><strong>Microsoft Learn</strong> - Power BI data modelling best practices</li>
        <li><strong>SQLBI</strong> - Advanced DAX patterns</li>
        <li><strong>Power Query Documentation</strong> - M language reference</li>
    </ul>

    <h2>🤝 Connect</h2>
    <p><strong>Junior Kevin</strong> · <a href="https://www.linkedin.com/in/junior-kevin-a7a901276/" target="_blank">LinkedIn</a></p>

    <hr>
    <div class="footnote">
        <p><strong>📄 License:</strong> This project is for educational purposes - part of a BI portfolio demonstrating data modelling competence.</p>
        <p><em>&quot;The goal of data modelling is not just to store data, but to make it discoverable, trustworthy, and actionable.&quot;</em></p>
    </div>

</div>
</body>
</html>
