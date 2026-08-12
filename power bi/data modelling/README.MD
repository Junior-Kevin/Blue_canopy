Data Modelling & Gold Layer Transformation

Power BI Semantic Model · Power Query (M) Transformations
📁 Project Overview

This document outlines the data modelling process for the Blue Canopy Retail Analytics Project. I built the entire dimensional model directly in Power BI using Power Query (M language). Starting from 29 raw silver-layer tables imported from SQL Server, I transformed them into a production-ready star schema.

Key Stats:

    ✅ 29 Silver Tables → 15 Gold Objects

    ✅ 6 Conformed Dimensions (shared across all data marts)

    ✅ 9 Atomic Fact Tables (transactional grain)

    ✅ 3 Aggregate Tables (pre-summed for performance)

    ✅ 1 Bridge Table (many-to-many resolution)

📁 Tables Before Data Modelling (Raw Silver Layer)

https://./raw_data_tables.png
📁 Tables After Data Modelling (Gold Layer)

https://./final_dimensional_data_model.png
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
