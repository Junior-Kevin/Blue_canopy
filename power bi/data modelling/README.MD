markdown

# 📊 Data Modelling & Gold Layer Transformation

**Power BI Semantic Model · Power Query (M) Transformations**

---

## 📁 Project Overview

This document outlines the data modelling process for the Blue Canopy Retail Analytics Project. I built the **entire dimensional model directly in Power BI** using Power Query (M language). Starting from 31 raw silver-layer tables imported from SQL Server, I transformed them into a production-ready **GALAXY SCHEMA**.

**Key Stats:**
- ✅ **31 Silver Tables** → **15 Gold Objects**
- ✅ **6 Conformed Dimensions** (shared across all data marts)
- ✅ **9 Atomic Fact Tables** (transactional grain)
- ✅ **3 Aggregate Tables** (pre-summed for performance)
- ✅ **2 Bridge Table** (many-to-many resolution) i.e dim_products_bridge and dim_stores_bridge

---

## 📁 Tables Before Data Modelling (Raw Silver Layer)

![Raw Data Tables](https://github.com/Junior-Kevin/Blue_canopy_-Business-Intelligence_project/blob/main/power%20bi/data%20modelling/raw_data_tables.png?raw=true)

---

## 📁 Tables After Data Modelling (Gold Layer)

![Final Dimensional Data Model](final_dimensional_data_model.png)

---

## 🏗️ Architecture Overview

SQL Server (Silver Layer)
↓ [Import Mode]
Power BI Desktop
↓ [Power Query - M Language]
Gold Layer (Power Query transforms)
↓ [Model Relationships]
Power BI Semantic Model (Star Schema)
↓ [DAX Measures]
5 Data Marts → Interactive Dashboards
text


---

## 📊 Gold Layer Objects Summary

### Dimension Tables (8 Total)

| Table | SCD Type | Purpose |
|-------|----------|---------|
| **dim_date** | - | Date calendar for all time-based analysis |
| **dim_customer** | Type 1 | Customer master for all customer-facing analysis |
| **dim_product** | Type 2 | Product master with historical price/cost tracking |
| **dim_store** | Type 2 | Store master with geography enrichment |
| **dim_employee** | Type 2 | Employee dimension for HR and sales attribution |
| **dim_supplier** | Type 1 | Supplier reference for procurement |
| **dim_campaign** | Type 1 | Marketing campaign attribution |
| **dim_geography_economic** | - | County geography with economic indicators |

### Fact Tables (9 Atomic)

| Table | Grain | Pattern | Key Dimensions |
|-------|-------|---------|----------------|
| **fact_pos_sales** | One row per POS line | Transaction | date, customer, product, store, employee |
| **fact_ecommerce_sales** | One row per order line | Transaction | date, customer, product, store |
| **fact_returns** | One row per return event | Transaction | date, product, store, customer |
| **fact_loyalty** | One row per loyalty event | Transaction | date, customer |
| **fact_inventory_movements** | One row per movement | Transaction | date, product, store |
| **fact_procurement** | One row per PO line | Accumulating Snapshot | date (3x), product, supplier |
| **fact_customer_experience** | One row per feedback | Transaction | date, customer |
| **fact_gift_card** | One row per transaction | Transaction | date, customer |
| **fact_time_attendance** | One row per clock event | Transaction | date, employee, store |

### Aggregate & Bridge Tables

| Table | Grain | Purpose |
|-------|-------|---------|
| **agg_store_financials** | One row per store per day | Fast P&L analysis |
| **agg_inventory_snapshot** | One row per product/store/day | Fast inventory position |
| **agg_competitor_benchmark** | One row per competitor per quarter | Market share analysis |
| **bridge_promotion_product** | One row per promotion-product pair | M2M resolution |

---

## 🏢 Data Marts (5 Subject Areas)

| Mart | Primary Fact Tables | Key Metrics |
|------|---------------------|-------------|
| **Sales & Revenue** | fact_pos_sales, fact_ecommerce_sales, fact_returns | Revenue, AOV, Gross Margin, Return Rate, Campaign ROI |
| **Customer & Loyalty** | fact_loyalty, fact_customer_experience | Active Customers, Churn Rate, NPS, Loyalty Redemption |
| **Inventory & Supply Chain** | fact_inventory_movements, fact_procurement, agg_inventory_snapshot | Stockouts, Turnover, Supplier OTD, Lead Time Variance |
| **Finance & Store P&L** | agg_store_financials | Net Profit, Operating Margin, Revenue/SqM |
| **HR & Workforce** | fact_time_attendance | Headcount, Retention Risk, Gender Diversity, Attendance |

---

## 🛠️ Tools Used

- **Power BI Desktop** - Data modelling, Power Query (M) transformations, DAX measures
- **SQL Server** - Source data (Silver Layer)
- **Tabular Editor** - Calculation groups and advanced modelling
- **DAX Studio** - Performance optimisation and measure testing

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Data Warehouse Bus Matrix — Enhanced</title>
    <style>
        /* ── reset & base ── */
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            background: #0b0b14;
            display: flex;
            justify-content: center;
            padding: 2rem 1rem;
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
        }

        .matrix-card {
            max-width: 1280px;
            width: 100%;
            background: #111122;
            border-radius: 24px;
            padding: 2rem 1.8rem 2rem;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.7);
            border: 1px solid #2a2a44;
        }

        /* ── header ── */
        .header {
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            flex-wrap: wrap;
            gap: 0.5rem 1rem;
            margin-bottom: 1.8rem;
            padding-bottom: 0.75rem;
            border-bottom: 2px solid #2a2a44;
        }

        .header h1 {
            font-size: 1.8rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            color: #e0e0ff;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .header h1 span {
            background: linear-gradient(135deg, #00f5ff, #8b5cf6);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        .header .sub {
            color: #7a7aaa;
            font-size: 0.85rem;
            font-weight: 400;
        }

        .badge {
            background: #1c1c3a;
            padding: 0.3rem 1rem;
            border-radius: 40px;
            font-size: 0.75rem;
            font-weight: 600;
            color: #a0a0d0;
            border: 1px solid #333366;
            letter-spacing: 0.02em;
        }

        /* ── legend ── */
        .legend {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 0.6rem 1.5rem;
            background: #16162e;
            padding: 0.6rem 1.2rem;
            border-radius: 60px;
            margin-bottom: 1.8rem;
            border: 1px solid #2a2a50;
        }

        .legend-item {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 0.75rem;
            font-weight: 500;
            color: #b0b0dd;
            transition: 0.15s;
        }

        .legend-item:hover {
            color: #ffffff;
        }

        .legend-swatch {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 22px;
            height: 22px;
            border-radius: 4px;
            font-size: 0.6rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            flex-shrink: 0;
        }

        /* ── matrix wrapper ── */
        .table-wrap {
            overflow-x: auto;
            border-radius: 16px;
            border: 1px solid #2a2a4a;
            background: #0d0d1a;
        }

        table {
            border-collapse: separate;
            border-spacing: 0;
            width: 100%;
            min-width: 800px;
            font-size: 0.78rem;
        }

        /* ── header cells ── */
        .th-corner {
            position: sticky;
            left: 0;
            z-index: 10;
            background: #1a1a38;
            border-right: 2px solid #2a2a4a;
            border-bottom: 2px solid #2a2a4a;
            padding: 0.8rem 1rem;
            min-width: 210px;
            width: 210px;
            text-align: left;
            font-weight: 600;
            color: #b0b0e0;
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }

        .th-corner .highlight {
            color: #00f5ff;
            font-weight: 700;
            display: block;
            font-size: 0.82rem;
            margin-top: 2px;
            text-transform: none;
            letter-spacing: 0;
        }

        .th-dim {
            background: #1a1a38;
            border-right: 1px solid #2a2a4a;
            border-bottom: 2px solid #2a2a4a;
            padding: 0.4rem 0.2rem;
            min-width: 70px;
            width: 70px;
            vertical-align: bottom;
            text-align: center;
        }

        .th-dim-inner {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 0.3rem;
        }

        .dim-label {
            writing-mode: vertical-rl;
            transform: rotate(180deg);
            font-size: 0.7rem;
            font-weight: 700;
            color: #d0d0ff;
            letter-spacing: 0.04em;
            height: 80px;
            white-space: nowrap;
        }

        .dim-badge {
            width: 30px;
            height: 30px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.55rem;
            font-weight: 800;
            flex-shrink: 0;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.4);
        }

        /* ── process row label ── */
        .td-process {
            position: sticky;
            left: 0;
            z-index: 5;
            background: #111122;
            border-right: 2px solid #2a2a4a;
            border-bottom: 1px solid #23233e;
            padding: 0.5rem 0.8rem;
            min-width: 210px;
            width: 210px;
            vertical-align: middle;
        }

        .td-process .pname {
            font-weight: 600;
            color: #e8e8ff;
            font-size: 0.82rem;
            margin-bottom: 0.1rem;
        }

        .td-process .pfact {
            font-size: 0.65rem;
            color: #6a6a9a;
            font-family: 'Courier New', monospace;
        }

        .td-process .ptag {
            display: inline-block;
            margin-top: 0.2rem;
            padding: 0.1rem 0.5rem;
            border-radius: 12px;
            font-size: 0.55rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }

        /* ── data cells ── */
        .td-cell {
            border-right: 1px solid #23233e;
            border-bottom: 1px solid #23233e;
            text-align: center;
            vertical-align: middle;
            padding: 0;
            min-width: 70px;
            width: 70px;
            height: 52px;
            transition: background 0.12s;
            background: #111122;
        }

        .td-cell.has-rel {
            cursor: pointer;
            background: #14142a;
        }

        .td-cell.has-rel:hover {
            background: #1e1e42;
        }

        .td-cell.hover-row {
            background: rgba(0, 245, 255, 0.05);
        }

        .td-cell.hover-col {
            background: rgba(139, 92, 246, 0.05);
        }

        .td-cell.hover-both {
            background: rgba(0, 245, 255, 0.12);
        }

        .cell-marker {
            width: 34px;
            height: 34px;
            border-radius: 6px;
            margin: 0 auto;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.7rem;
            font-weight: 800;
            transition: transform 0.1s, box-shadow 0.15s;
            box-shadow: 0 2px 6px rgba(0, 0, 0, 0.3);
        }

        .td-cell.has-rel:hover .cell-marker {
            transform: scale(1.15);
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.6);
        }

        .cell-empty {
            color: #2a2a50;
            font-size: 1.2rem;
            opacity: 0.4;
        }

        /* ── tooltip ── */
        #tooltip {
            position: fixed;
            background: #1a1a3a;
            border: 1px solid #44447a;
            border-radius: 12px;
            padding: 0.9rem 1.1rem;
            max-width: 320px;
            z-index: 9999;
            pointer-events: none;
            display: none;
            box-shadow: 0 12px 48px rgba(0, 0, 0, 0.7);
            backdrop-filter: blur(4px);
        }

        #tooltip .tt-title {
            font-weight: 700;
            color: #f0f0ff;
            font-size: 0.9rem;
            margin-bottom: 0.25rem;
        }

        #tooltip .tt-rel {
            font-size: 0.7rem;
            font-weight: 700;
            padding: 0.15rem 0.6rem;
            border-radius: 20px;
            display: inline-block;
            margin-bottom: 0.4rem;
        }

        #tooltip .tt-desc {
            color: #c0c0ee;
            font-size: 0.78rem;
            line-height: 1.5;
        }

        #tooltip .tt-key {
            margin-top: 0.5rem;
            font-size: 0.7rem;
            font-family: 'Courier New', monospace;
            color: #8888bb;
            background: #0d0d22;
            padding: 0.2rem 0.6rem;
            border-radius: 4px;
            border: 1px solid #333366;
            word-break: break-all;
        }

        /* ── summary bar ── */
        .summary-bar {
            display: flex;
            flex-wrap: wrap;
            gap: 0.8rem;
            margin-top: 1.6rem;
        }

        .summary-card {
            background: #16162e;
            border: 1px solid #2a2a4a;
            border-radius: 12px;
            padding: 0.6rem 1.2rem;
            flex: 1;
            min-width: 110px;
            text-align: center;
        }

        .summary-card .val {
            font-size: 1.6rem;
            font-weight: 700;
            color: #00f5ff;
            line-height: 1.2;
        }

        .summary-card .lbl {
            font-size: 0.65rem;
            color: #7a7aaa;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            margin-top: 0.1rem;
        }

        .summary-card .val.green {
            color: #00ff88;
        }
        .summary-card .val.purple {
            color: #a78bfa;
        }
        .summary-card .val.orange {
            color: #fb923c;
        }
        .summary-card .val.pink {
            color: #f472b6;
        }

        /* ── responsive ── */
        @media (max-width: 640px) {
            .matrix-card {
                padding: 1rem;
            }
            .header h1 {
                font-size: 1.3rem;
            }
            .th-corner,
            .td-process {
                min-width: 140px;
                width: 140px;
            }
            .td-cell,
            .th-dim {
                min-width: 56px;
                width: 56px;
            }
            .th-corner {
                font-size: 0.6rem;
                padding: 0.4rem 0.6rem;
            }
            .td-process .pname {
                font-size: 0.7rem;
            }
            .summary-card .val {
                font-size: 1.2rem;
            }
            .legend {
                gap: 0.4rem 0.8rem;
                padding: 0.4rem 0.8rem;
                border-radius: 20px;
            }
            .legend-item {
                font-size: 0.65rem;
            }
        }
    </style>
</head>
<body>

<div class="matrix-card">

    <!-- ─── HEADER ─── -->
    <div class="header">
        <div>
            <h1>⬡ <span>Data Warehouse Bus Matrix</span></h1>
            <div class="sub">Gold Layer · Galaxy Schema · 12 Processes × 10 Dimensions</div>
        </div>
        <div class="badge"><span style="color:#00f5ff;">●</span> Kenya Retail BI</div>
    </div>

    <!-- ─── LEGEND ─── -->
    <div class="legend">
        <span class="legend-item"><span class="legend-swatch" style="background:rgba(0,245,255,0.18);border:1.5px solid #00f5ff;color:#00f5ff;">PK</span> Direct FK</span>
        <span class="legend-item"><span class="legend-swatch" style="background:rgba(191,0,255,0.18);border:1.5px solid #bf00ff;color:#bf00ff;">RP</span> Role-Playing</span>
        <span class="legend-item"><span class="legend-swatch" style="background:rgba(0,255,136,0.18);border:1.5px solid #00ff88;color:#00ff88;">SN</span> Snapshot</span>
        <span class="legend-item"><span class="legend-swatch" style="background:rgba(255,214,0,0.18);border:1.5px solid #ffd600;color:#ffd600;">BR</span> Bridge</span>
        <span class="legend-item"><span class="legend-swatch" style="background:rgba(255,107,0,0.18);border:1.5px solid #ff6b00;color:#ff6b00;">DG</span> Degenerate</span>
        <span class="legend-item"><span class="legend-swatch" style="background:rgba(255,45,107,0.18);border:1.5px solid #ff2d6b;color:#ff2d6b;">OT</span> Outrigger</span>
    </div>

    <!-- ─── MATRIX ─── -->
    <div class="table-wrap">
        <table id="bus-matrix"></table>
    </div>

    <!-- ─── SUMMARY ─── -->
    <div class="summary-bar" id="summary-bar"></div>

</div>

<!-- ─── TOOLTIP ─── -->
<div id="tooltip">
    <div class="tt-title" id="tt-title"></div>
    <div class="tt-rel" id="tt-rel"></div>
    <div class="tt-desc" id="tt-desc"></div>
    <div class="tt-key" id="tt-key"></div>
</div>

<script>
    // ─── DATA ────────────────────────────────────────────────

    const DIMS = [
        { id: 'date', label: 'dim_date', abbr: 'DT', color: '#00f5ff', desc: 'Conformed date — shared by ALL facts' },
        { id: 'customer', label: 'dim_customer', abbr: 'CU', color: '#bf00ff', desc: 'Customer identity, segment, tier, county' },
        { id: 'product', label: 'dim_product', abbr: 'PR', color: '#00ff88', desc: 'Product catalogue — SCD Type 2' },
        { id: 'store', label: 'dim_store', abbr: 'ST', color: '#ffd600', desc: 'Store locations — SCD Type 2, GIS-enriched' },
        { id: 'employee', label: 'dim_employee', abbr: 'EM', color: '#ff6b00', desc: 'Employee master — SCD Type 2' },
        { id: 'supplier', label: 'dim_supplier', abbr: 'SU', color: '#ff2d6b', desc: 'Supplier reference — current version' },
        { id: 'campaign', label: 'dim_campaign', abbr: 'CA', color: '#00bfff', desc: 'Marketing campaign — date-range attribution' },
        { id: 'promotion', label: 'dim_promotion', abbr: 'PM', color: '#ff00c8', desc: 'Promotions — via bridge_promotion_product' },
        { id: 'geo_eco', label: 'dim_geo_econ', abbr: 'GE', color: '#a0ff00', desc: 'County geography + economic indicators' },
        { id: 'behaviour', label: 'dim_cust_beh', abbr: 'CB', color: '#ff8c00', desc: 'Customer mini-dimension — volatile attributes' },
    ];

    const PROCESSES = [{
        id: 'pos_sales',
        name: 'In-Store POS Sales',
        fact: 'fact_pos_sales',
        domain: 'Sales',
        tagColor: '#00ff88',
        rels: {
            date: { type: 'PK', join: 'date_sk → dim_date.date_sk', note: 'Transaction date. Every sale must have a date.' },
            customer: { type: 'PK', join: 'customer_sk → dim_customer.customer_sk', note: 'Nullable — walk-in maps to SK=-1.' },
            product: { type: 'PK', join: 'product_sk → dim_product.product_sk (SCD-2 date-range)', note: 'Correct cost/price version.' },
            store: { type: 'PK', join: 'store_sk → dim_store.store_sk (SCD-2)', note: 'Store version active on transaction date.' },
            employee: { type: 'PK', join: 'employee_sk → dim_employee.employee_sk (cashier)', note: 'Cashier who processed transaction.' },
            campaign: { type: 'PK', join: 'campaign_sk → dim_campaign.campaign_sk', note: 'Date-range overlap attribution.' },
            promotion: { type: 'BR', join: 'via bridge_promotion_product → dim_promotion', note: 'Resolved through bridge table.' },
            geo_eco: { type: 'OT', join: 'dim_store.county → dim_geo_econ.county', note: 'County economic context.' },
            behaviour: { type: 'PK', join: 'behaviour_sk → dim_customer_behaviour.behaviour_sk', note: 'Customer tier/segment at time of purchase.' },
        }
    }, {
        id: 'ecom_sales',
        name: 'E-Commerce Sales',
        fact: 'fact_ecommerce_sales',
        domain: 'Sales',
        tagColor: '#00ff88',
        rels: {
            date: { type: 'PK', join: 'date_sk → dim_date.date_sk', note: 'Order placement date.' },
            customer: { type: 'PK', join: 'customer_sk → dim_customer.customer_sk', note: 'E-commerce always has a customer.' },
            product: { type: 'PK', join: 'product_sk → dim_product.product_sk (SCD-2)', note: 'SCD-2 version on order date.' },
            campaign: { type: 'PK', join: 'campaign_sk → dim_campaign.campaign_sk', note: 'Campaign active on order date.' },
            promotion: { type: 'BR', join: 'via bridge_promotion_product → dim_promotion', note: 'Promotions covering product.' },
            geo_eco: { type: 'OT', join: 'dim_customer.county → dim_geo_econ.county', note: 'Customer county context.' },
            behaviour: { type: 'PK', join: 'behaviour_sk → dim_customer_behaviour.behaviour_sk', note: 'Customer profile at order time.' },
        }
    }, {
        id: 'returns',
        name: 'Product Returns',
        fact: 'fact_returns',
        domain: 'Sales',
        tagColor: '#00ff88',
        rels: {
            date: { type: 'RP', join: 'return_date_sk + orig_sale_date_sk → dim_date', note: 'Two date roles: return date & original sale date.' },
            customer: { type: 'OT', join: 'via original POS transaction → dim_customer', note: 'Derived from original sale.' },
            product: { type: 'PK', join: 'product_sk → dim_product.product_sk', note: 'Returned product.' },
            store: { type: 'OT', join: 'via original POS transaction → dim_store', note: 'Derived from original sale store.' },
        }
    }, {
        id: 'loyalty',
        name: 'Loyalty Programme',
        fact: 'fact_loyalty',
        domain: 'Customer',
        tagColor: '#bf00ff',
        rels: {
            date: { type: 'PK', join: 'date_sk → dim_date.date_sk', note: 'Loyalty event date.' },
            customer: { type: 'PK', join: 'customer_sk → dim_customer.customer_sk', note: 'Every loyalty event belongs to a customer.' },
            campaign: { type: 'OT', join: 'via source order → dim_campaign', note: 'Campaign attribution via originating order.' },
            behaviour: { type: 'PK', join: 'behaviour_sk → dim_customer_behaviour.behaviour_sk', note: 'Customer tier at event time.' },
        }
    }, {
        id: 'cx',
        name: 'Customer Experience',
        fact: 'fact_customer_experience',
        domain: 'Customer',
        tagColor: '#bf00ff',
        rels: {
            date: { type: 'PK', join: 'date_sk → dim_date.date_sk', note: 'Feedback or interaction date.' },
            customer: { type: 'PK', join: 'customer_sk → dim_customer.customer_sk', note: 'Every CX record belongs to a customer.' },
            store: { type: 'OT', join: 'via customer → nearest store', note: 'Store attribution for CX reporting.' },
            geo_eco: { type: 'OT', join: 'dim_customer.county → dim_geo_econ.county', note: 'County economic context for satisfaction.' },
        }
    }, {
        id: 'inventory',
        name: 'Inventory Movements',
        fact: 'fact_inventory_movements',
        domain: 'Supply Chain',
        tagColor: '#ff2d6b',
        rels: {
            date: { type: 'PK', join: 'date_sk → dim_date.date_sk', note: 'Movement event date.' },
            product: { type: 'PK', join: 'product_sk → dim_product.product_sk', note: 'SCD-2 matched to movement date.' },
            store: { type: 'PK', join: 'store_sk → dim_store.store_sk', note: 'Store where movement occurred.' },
            supplier: { type: 'OT', join: 'dim_product.supplier_sk → dim_supplier', note: 'Supplier via product outrigger.' },
        }
    }, {
        id: 'inv_snap',
        name: 'Inventory Snapshot',
        fact: 'agg_inventory_snapshot',
        domain: 'Supply Chain',
        tagColor: '#ff2d6b',
        rels: {
            date: { type: 'SN', join: 'date_sk → dim_date.date_sk', note: 'Snapshot date — periodic position table.' },
            product: { type: 'SN', join: 'product_sk → dim_product.product_sk', note: 'Product version at snapshot.' },
            store: { type: 'SN', join: 'store_sk → dim_store.store_sk', note: 'Store holding stock at snapshot.' },
            supplier: { type: 'OT', join: 'dim_product.supplier_sk → dim_supplier', note: 'Supplier via product outrigger.' },
        }
    }, {
        id: 'procurement',
        name: 'Procurement (PO)',
        fact: 'fact_procurement',
        domain: 'Supply Chain',
        tagColor: '#ff2d6b',
        rels: {
            date: { type: 'RP', join: 'order_date_sk + expected_delivery_date_sk + receipt_date_sk → dim_date', note: 'Three date roles: order, expected, receipt.' },
            product: { type: 'PK', join: 'product_sk → dim_product.product_sk', note: 'Product ordered on PO line.' },
            supplier: { type: 'PK', join: 'supplier_sk → dim_supplier.supplier_sk', note: 'Direct FK — every PO line has a supplier.' },
            store: { type: 'OT', join: 'via receiving store', note: 'Store for procurement cost allocation.' },
        }
    }, {
        id: 'fin',
        name: 'Store Daily Financials',
        fact: 'agg_store_financials',
        domain: 'Finance',
        tagColor: '#ffd600',
        rels: {
            date: { type: 'SN', join: 'date_sk → dim_date.date_sk', note: 'Daily financial snapshot — aggregation table.' },
            store: { type: 'SN', join: 'store_sk → dim_store.store_sk', note: 'Store P&L.' },
            geo_eco: { type: 'OT', join: 'dim_store.county → dim_geo_econ.county', note: 'Economic context for correlation.' },
        }
    }, {
        id: 'gift_card',
        name: 'Gift Card Transactions',
        fact: 'fact_gift_card',
        domain: 'Finance',
        tagColor: '#ffd600',
        rels: {
            date: { type: 'PK', join: 'date_sk → dim_date.date_sk', note: 'Transaction date.' },
            customer: { type: 'OT', join: 'via card_number → crm', note: 'Customer linkage where available.' },
            store: { type: 'OT', join: 'via POS redemption', note: 'Redemption store derivable.' },
        }
    }, {
        id: 'time_att',
        name: 'Time & Attendance',
        fact: 'fact_time_attendance',
        domain: 'HR',
        tagColor: '#ff6b00',
        rels: {
            date: { type: 'PK', join: 'date_sk → dim_date.date_sk', note: 'Clock event date.' },
            store: { type: 'PK', join: 'store_sk → dim_store.store_sk', note: 'Store where employee clocked in.' },
            employee: { type: 'PK', join: 'employee_sk → dim_employee.employee_sk (SCD-2)', note: 'SCD-2 matched to event date.' },
        }
    }, {
        id: 'competitor',
        name: 'Competitor Benchmarks',
        fact: 'agg_competitor_benchmark',
        domain: 'Finance',
        tagColor: '#ffd600',
        rels: {
            date: { type: 'DG', join: 'year + quarter stored as degenerate', note: 'No direct date_sk FK — year/quarter as degenerate dims.' },
            geo_eco: { type: 'OT', join: 'correlated via quarter → economic', note: 'Market share vs economic indicators.' },
        }
    }];

    // ─── RENDER ──────────────────────────────────────────────

    const REL_TYPES = {
        PK: { label: 'PK', color: '#00f5ff', bg: 'rgba(0,245,255,0.18)' },
        RP: { label: 'RP', color: '#bf00ff', bg: 'rgba(191,0,255,0.18)' },
        SN: { label: 'SN', color: '#00ff88', bg: 'rgba(0,255,136,0.18)' },
        BR: { label: 'BR', color: '#ffd600', bg: 'rgba(255,214,0,0.18)' },
        DG: { label: 'DG', color: '#ff6b00', bg: 'rgba(255,107,0,0.18)' },
        OT: { label: 'OT', color: '#ff2d6b', bg: 'rgba(255,45,107,0.18)' },
    };

    let hoverRow = null,
        hoverCol = null;

    function buildMatrix() {
        const table = document.getElementById('bus-matrix');
        table.innerHTML = '';

        // ── header ──
        const thead = document.createElement('thead');
        const trHead = document.createElement('tr');

        const corner = document.createElement('th');
        corner.className = 'th-corner';
        corner.innerHTML = `Business Process <span class="highlight">Gold Layer Fact</span>`;
        trHead.appendChild(corner);

        DIMS.forEach((dim, ci) => {
            const th = document.createElement('th');
            th.className = 'th-dim';
            th.dataset.col = ci;
            th.innerHTML = `
            <div class="th-dim-inner">
              <div class="dim-badge" style="background:${dim.color}22;border:2px solid ${dim.color};color:${dim.color};">
                ${dim.abbr}
              </div>
              <span class="dim-label">${dim.label}</span>
            </div>`;
            th.title = dim.desc;
            trHead.appendChild(th);
        });
        thead.appendChild(trHead);
        table.appendChild(thead);

        // ── body ──
        const tbody = document.createElement('tbody');
        PROCESSES.forEach((proc, ri) => {
            const tr = document.createElement('tr');
            tr.dataset.row = ri;

            const tdProc = document.createElement('td');
            tdProc.className = 'td-process';
            tdProc.innerHTML = `
            <div class="pname">${proc.name}</div>
            <div class="pfact">${proc.fact}</div>
            <span class="ptag" style="background:${proc.tagColor}22;color:${proc.tagColor};">${proc.domain}</span>`;
            tr.appendChild(tdProc);

            DIMS.forEach((dim, ci) => {
                const td = document.createElement('td');
                td.className = 'td-cell';
                td.dataset.row = ri;
                td.dataset.col = ci;

                const rel = proc.rels[dim.id];
                if (rel) {
                    td.classList.add('has-rel');
                    const rt = REL_TYPES[rel.type];
                    td.innerHTML = `
                  <div class="cell-marker" style="background:${rt.bg};border:2px solid ${rt.color};color:${rt.color};">
                    ${rt.label}
                  </div>`;

                    td.addEventListener('mouseenter', (e) => {
                        hoverRow = ri;
                        hoverCol = ci;
                        updateHighlight();
                        showTooltip(e, proc, dim, rel, rt);
                    });
                    td.addEventListener('mousemove', (e) => moveTooltip(e));
                    td.addEventListener('mouseleave', () => {
                        hoverRow = null;
                        hoverCol = null;
                        updateHighlight();
                        hideTooltip();
                    });
                } else {
                    td.innerHTML = `<div class="cell-empty">·</div>`;
                }
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        buildSummary();
    }

    function updateHighlight() {
        document.querySelectorAll('.td-cell').forEach(td => {
            const r = parseInt(td.dataset.row);
            const c = parseInt(td.dataset.col);
            td.classList.remove('hover-row', 'hover-col', 'hover-both');
            if (hoverRow === null) return;
            const rowMatch = (r === hoverRow);
            const colMatch = (c === hoverCol);
            if (rowMatch && colMatch) td.classList.add('hover-both');
            else if (rowMatch) td.classList.add('hover-row');
            else if (colMatch) td.classList.add('hover-col');
        });
    }

    function showTooltip(e, proc, dim, rel, rt) {
        const tt = document.getElementById('tooltip');
        document.getElementById('tt-title').textContent = `${proc.name} ↔ ${dim.label}`;
        const relEl = document.getElementById('tt-rel');
        relEl.textContent = rt.label + ' · ' + (rel.type === 'PK' ? 'Direct FK' : rel.type === 'RP' ? 'Role-Playing' : rel.type === 'SN' ? 'Periodic Snapshot' : rel.type === 'BR' ? 'Bridge Table' : rel.type === 'DG' ? 'Degenerate' : 'Outrigger');
        relEl.style.background = rt.bg;
        relEl.style.color = rt.color;
        relEl.style.border = `1px solid ${rt.color}`;
        document.getElementById('tt-desc').textContent = rel.note;
        document.getElementById('tt-key').textContent = rel.join;
        tt.style.display = 'block';
        moveTooltip(e);
    }

    function moveTooltip(e) {
        const tt = document.getElementById('tooltip');
        let x = e.clientX + 16;
        let y = e.clientY + 16;
        if (x + 340 > window.innerWidth) x = e.clientX - 340;
        if (y + 200 > window.innerHeight) y = e.clientY - 180;
        tt.style.left = x + 'px';
        tt.style.top = y + 'px';
    }

    function hideTooltip() {
        document.getElementById('tooltip').style.display = 'none';
    }

    function buildSummary() {
        let totalRels = 0;
        let typeCounts = {};
        PROCESSES.forEach(p => {
            Object.values(p.rels).forEach(r => {
                totalRels++;
                typeCounts[r.type] = (typeCounts[r.type] || 0) + 1;
            });
        });

        const bar = document.getElementById('summary-bar');
        bar.innerHTML = `
          <div class="summary-card"><div class="val">${PROCESSES.length}</div><div class="lbl">Processes</div></div>
          <div class="summary-card"><div class="val purple">${DIMS.length}</div><div class="lbl">Dimensions</div></div>
          <div class="summary-card"><div class="val green">${totalRels}</div><div class="lbl">Relationships</div></div>
          <div class="summary-card"><div class="val">${typeCounts['PK']||0}</div><div class="lbl">Direct FK</div></div>
          <div class="summary-card"><div class="val orange">${typeCounts['RP']||0}</div><div class="lbl">Role-Playing</div></div>
          <div class="summary-card"><div class="val pink">${(typeCounts['SN']||0)+(typeCounts['BR']||0)+(typeCounts['DG']||0)+(typeCounts['OT']||0)}</div><div class="lbl">Advanced Patterns</div></div>
        `;
    }

    buildMatrix();
</script>
</body>
</html>
---

## 🤝 Connect

**Junior Kevin** · [LinkedIn](https://www.linkedin.com/in/junior-kevin-a7a901276/)

---

*"The goal of data modelling is not just to store data, but to make it discoverable, trustworthy, and actionable."*
