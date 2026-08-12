
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

---

## 🤝 Connect

**Junior Kevin** · [LinkedIn](https://www.linkedin.com/in/junior-kevin-a7a901276/)

---

*"The goal of data modelling is not just to store data, but to make it discoverable, trustworthy, and actionable."*
