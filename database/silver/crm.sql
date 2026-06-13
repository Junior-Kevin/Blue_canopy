-- ===================================================================
-- SILVER LAYER: CRM DATA TRANSFORMATION (With Deduplication)
-- ===================================================================

DECLARE @start DATE = '1978-01-01';
DECLARE @end DATE = '2010-12-31';
DECLARE @date_span INT = DATEDIFF(DAY, @start, @end) + 1;

-- Drop and recreate the silver table
DROP TABLE IF EXISTS Blue_canopy.silver.crm;

CREATE TABLE Blue_canopy.silver.crm (
    customer_id INT PRIMARY KEY,
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    full_name NVARCHAR(201),
    gender VARCHAR(20),
    birth_date DATE,
    age INT,
    age_band VARCHAR(20),
    phone VARCHAR(50),
    email VARCHAR(255),
    county VARCHAR(100),
    town VARCHAR(100),
    customer_segment VARCHAR(50),
    acquisition_channel VARCHAR(50),
    registration_date DATE,
    churn_date DATE,
    loyalty_tier VARCHAR(50),
    communication_preferences VARCHAR(100),
    feedback_score DECIMAL(5,2),
    home_county VARCHAR(100),
    primary_store_id INT,
    is_churned BIT,
    tenure_days INT,
    tenure_months INT,
    tenure_band VARCHAR(30),
    registration_year INT,
    registration_month INT,
    registration_quarter INT,
    email_domain VARCHAR(100),
    phone_prefix VARCHAR(10),
    is_phone_valid BIT,
    is_email_valid BIT,
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- Insert transformed data with deduplication
WITH 
source_cleaned AS (
    SELECT 
        customer_id_clean = TRY_CAST(
            CASE 
                WHEN customer_id LIKE 'CUST-%' THEN REPLACE(customer_id, 'CUST-', '')
                WHEN customer_id LIKE '%DUP%' THEN NULL
                ELSE customer_id
            END AS INT
        ),
        first_name = TRIM(UPPER(LEFT(LOWER(first_name), 1)) + LOWER(SUBSTRING(first_name, 2, LEN(first_name)))),
        last_name = TRIM(UPPER(LEFT(LOWER(last_name), 1)) + LOWER(SUBSTRING(last_name, 2, LEN(last_name)))),
        gender = CASE 
            WHEN gender IN ('M', 'Male', 'MALE', 'male') THEN 'Male'
            WHEN gender IN ('F', 'Female', 'FEMALE', 'female') THEN 'Female'
            ELSE 'Other'
        END,
        raw_birth_date = birth_date,
        raw_registration_date = registration_date,
        raw_churn_date = churn_date,
        phone = TRIM(REPLACE(REPLACE(REPLACE(phone, ' ', ''), '-', ''), '+', '')),
        email = TRIM(LOWER(email)),
        county = TRIM(UPPER(LEFT(county, 1)) + LOWER(SUBSTRING(county, 2, LEN(county)))),
        town = TRIM(UPPER(LEFT(town, 1)) + LOWER(SUBSTRING(town, 2, LEN(town)))),
        customer_segment = CASE 
            WHEN customer_segment IN ('Platinum', 'Gold', 'Silver', 'Bronze') THEN customer_segment
            ELSE 'Standard'
        END,
        acquisition_channel = CASE 
            WHEN acquisition_channel IN ('Online', 'Store', 'Referral', 'Social Media', 'Email') THEN acquisition_channel
            ELSE 'Other'
        END,
        loyalty_tier = CASE 
            WHEN loyalty_tier IN ('Platinum', 'Gold', 'Silver', 'Bronze') THEN loyalty_tier
            ELSE 'Bronze'
        END,
        communication_preferences = COALESCE(communication_preferences, 'Email'),
        feedback_score = TRY_CAST(feedback_score AS DECIMAL(5,2)),
        random_days = ABS(CHECKSUM(NEWID())) % @date_span,
        -- Create a row number to identify duplicates
        row_num = ROW_NUMBER() OVER(
            PARTITION BY 
                TRY_CAST(
                    CASE 
                        WHEN customer_id LIKE 'CUST-%' THEN REPLACE(customer_id, 'CUST-', '')
                        ELSE customer_id
                    END AS INT
                ) 
            ORDER BY 
                CASE WHEN churn_date IS NULL THEN 1 ELSE 2 END,  -- Prefer active customers
                registration_date DESC  -- Prefer most recent registration
        )
    FROM Blue_canopy.bronze.crm_raw
    WHERE customer_id NOT LIKE '%DUP%'
      AND TRY_CAST(
            CASE 
                WHEN customer_id LIKE 'CUST-%' THEN REPLACE(customer_id, 'CUST-', '')
                ELSE customer_id
            END AS INT) IS NOT NULL
),
-- Keep only the first row per customer_id
deduplicated AS (
    SELECT *
    FROM source_cleaned
    WHERE row_num = 1
),
date_converted AS (
    SELECT 
        *,
        clean_birth_date = CASE 
            WHEN ISDATE(raw_birth_date) = 1 AND raw_birth_date NOT LIKE '%[^0-9-]%'
                 AND raw_birth_date NOT IN ('2023-13-45', '1900-01-01')
            THEN CAST(raw_birth_date AS DATE)
            ELSE DATEADD(DAY, random_days, @start)
        END,
        clean_registration_date = CASE 
            WHEN ISDATE(raw_registration_date) = 1 AND raw_registration_date NOT LIKE '%[^0-9-]%'
                 AND raw_registration_date NOT IN ('2023-13-45', '1900-01-01')
            THEN CAST(raw_registration_date AS DATE)
            ELSE DATEADD(DAY, random_days, @start)
        END,
        clean_churn_date = CASE 
            WHEN raw_churn_date IS NULL THEN NULL
            WHEN ISDATE(raw_churn_date) = 1 AND raw_churn_date NOT LIKE '%[^0-9-]%'
                 AND raw_churn_date NOT IN ('2023-13-45', '1900-01-01')
            THEN CAST(raw_churn_date AS DATE)
            ELSE DATEADD(DAY, random_days, @start)
        END
    FROM deduplicated
)
INSERT INTO Blue_canopy.silver.crm (
    customer_id, first_name, last_name, full_name, gender, birth_date, age, age_band,
    phone, email, county, town, customer_segment, acquisition_channel,
    registration_date, churn_date, loyalty_tier, communication_preferences, feedback_score,
    home_county, primary_store_id, is_churned, tenure_days, tenure_months, tenure_band,
    registration_year, registration_month, registration_quarter, email_domain,
    phone_prefix, is_phone_valid, is_email_valid
)
SELECT 
    customer_id_clean,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name),
    gender,
    clean_birth_date,
    DATEDIFF(YEAR, clean_birth_date, GETDATE()),
    CASE 
        WHEN clean_birth_date IS NULL THEN 'Unknown'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) < 18 THEN 'Under 18'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) BETWEEN 18 AND 24 THEN '18-24'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) BETWEEN 25 AND 34 THEN '25-34'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) BETWEEN 35 AND 49 THEN '35-49'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) BETWEEN 50 AND 64 THEN '50-64'
        ELSE '65+'
    END,
    CASE 
        WHEN LEN(phone) = 9 AND phone LIKE '7%' THEN CONCAT('07', phone)
        WHEN LEN(phone) = 9 AND phone LIKE '1%' THEN CONCAT('01', phone)
        WHEN LEN(phone) = 10 AND phone LIKE '07%' THEN phone
        WHEN LEN(phone) = 12 AND phone LIKE '2547%' THEN CONCAT('0', RIGHT(phone, 9))
        ELSE phone
    END,
    email,
    county,
    town,
    customer_segment,
    acquisition_channel,
    clean_registration_date,
    clean_churn_date,
    loyalty_tier,
    communication_preferences,
    feedback_score,
    NULL,  -- home_county
    NULL,  -- primary_store_id
    CASE WHEN clean_churn_date IS NOT NULL AND clean_churn_date <= GETDATE() THEN 1 ELSE 0 END,
    DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())),
    DATEDIFF(MONTH, clean_registration_date, ISNULL(clean_churn_date, GETDATE())),
    CASE 
        WHEN DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) < 30 THEN 'New (<30 days)'
        WHEN DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) < 90 THEN 'Recent (30-90 days)'
        WHEN DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) < 180 THEN 'Regular (3-6 months)'
        WHEN DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) < 365 THEN 'Established (6-12 months)'
        ELSE 'Loyal (>1 year)'
    END,
    YEAR(clean_registration_date),
    MONTH(clean_registration_date),
    DATEPART(QUARTER, clean_registration_date),
    RIGHT(email, LEN(email) - CHARINDEX('@', email)),
    LEFT(phone, 3),
    CASE WHEN LEN(phone) BETWEEN 10 AND 12 AND phone NOT LIKE '%[^0-9]%' THEN 1 ELSE 0 END,
    CASE WHEN email LIKE '%_@__%.__%' THEN 1 ELSE 0 END
FROM date_converted
WHERE customer_id_clean IS NOT NULL;

-- Verify no duplicates were inserted
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_customers,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT customer_id) 
        THEN 'No duplicates - Good!' 
        ELSE 'Has duplicates - Problem!' 
    END AS duplicate_check
FROM Blue_canopy.silver.crm;
