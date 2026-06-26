-- ===================================================================
-- SILVER LAYER: CRM DATA TRANSFORMATION (Preserve ID Format)
-- ===================================================================

DECLARE @start DATE = '1978-01-01';
DECLARE @end DATE = '2010-12-31';
DECLARE @date_span INT = DATEDIFF(DAY, @start, @end) + 1;

-- Drop and recreate the silver table
DROP TABLE IF EXISTS Blue_canopy.silver.crm;

CREATE TABLE Blue_canopy.silver.crm (
    customer_id NVARCHAR(50) PRIMARY KEY,
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

-- Insert transformed data - ONLY remove duplicates, keep ID format
WITH 
-- First, remove duplicates by keeping the first occurrence of each customer_id
deduplicated AS (
    SELECT 
        customer_id,  -- Keep original format
        first_name,
        last_name,
        gender,
        birth_date,
        phone,
        email,
        county,
        town,
        customer_segment,
        acquisition_channel,
        registration_date,
        churn_date,
        loyalty_tier,
        communication_preferences,
        feedback_score,
        -- Create row number to identify and remove duplicates
        ROW_NUMBER() OVER(
            PARTITION BY customer_id  -- Group by original customer_id
            ORDER BY 
                CASE WHEN churn_date IS NULL THEN 1 ELSE 2 END,  -- Prefer active customers first
                registration_date DESC  -- Then most recent registration
        ) AS row_num
    FROM Blue_canopy.bronze.crm_raw
    WHERE customer_id NOT LIKE '%DUP%'  -- Remove rows with DUP marker
      AND customer_id IS NOT NULL
),
-- Clean the data after deduplication
cleaned AS (
    SELECT 
        customer_id,
        -- Clean names (preserve format, just capitalize first letter)
        first_name = TRIM(UPPER(LEFT(LOWER(first_name), 1)) + LOWER(SUBSTRING(first_name, 2, LEN(first_name)))),
        last_name = TRIM(UPPER(LEFT(LOWER(last_name), 1)) + LOWER(SUBSTRING(last_name, 2, LEN(last_name)))),
        -- Standardize gender
		gender = CASE 
		             WHEN (len(first_name) + len(last_name))%2 = 1 THEN 'Male'
				     ELSE 'Female'
		         END,
        raw_birth_date = birth_date,
        raw_registration_date = registration_date,
        raw_churn_date = churn_date,
        -- Clean phone
        phone = TRIM(REPLACE(REPLACE(REPLACE(phone, ' ', ''), '-', ''), '+', '')),
        -- Clean email
        email = TRIM(LOWER(replace(email,'example','gmail'))),
        -- Clean geography
        county = TRIM(UPPER(LEFT(county, 1)) + LOWER(SUBSTRING(county, 2, LEN(county)))),
        town = TRIM(UPPER(LEFT(town, 1)) + LOWER(SUBSTRING(town, 2, LEN(town)))),
        -- Standardize segments
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
        random_days = ABS(CHECKSUM(NEWID())) % @date_span
    FROM deduplicated
    WHERE row_num = 1  -- Keep only first occurrence of each customer_id
),
-- Convert dates
date_converted AS (
    SELECT 
        *,
        clean_birth_date = CASE 
            WHEN ISDATE(raw_birth_date) = 1 
                 AND raw_birth_date NOT LIKE '%[^0-9-]%'
                 AND raw_birth_date NOT IN ('2023-13-45', '1900-01-01')
            THEN CAST(raw_birth_date AS DATE)
            ELSE DATEADD(DAY, random_days, @start)
        END,
        clean_registration_date = CASE 
            WHEN ISDATE(raw_registration_date) = 1 
                 AND raw_registration_date NOT LIKE '%[^0-9-]%'
                 AND raw_registration_date NOT IN ('2023-13-45', '1900-01-01')
            THEN CAST(raw_registration_date AS DATE)
            ELSE DATEADD(DAY, random_days, @start)
        END,
        clean_churn_date = CASE 
            WHEN raw_churn_date IS NULL THEN NULL
            WHEN ISDATE(raw_churn_date) = 1 
                 AND raw_churn_date NOT LIKE '%[^0-9-]%'
                 AND raw_churn_date NOT IN ('2023-13-45', '1900-01-01')
            THEN CAST(raw_churn_date AS DATE)
            ELSE DATEADD(DAY, random_days, @start)
        END
    FROM cleaned
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
    customer_id,  -- Original format preserved!
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    gender,
    clean_birth_date AS birth_date,
    DATEDIFF(YEAR, clean_birth_date, GETDATE()) AS age,
    CASE 
        WHEN clean_birth_date IS NULL THEN 'Unknown'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) < 18 THEN 'Under 18'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) BETWEEN 18 AND 24 THEN '18-24'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) BETWEEN 25 AND 34 THEN '25-34'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) BETWEEN 35 AND 49 THEN '35-49'
        WHEN DATEDIFF(YEAR, clean_birth_date, GETDATE()) BETWEEN 50 AND 64 THEN '50-64'
        ELSE '65+'
    END AS age_band,
    -- Format phone number (keeps original digits, just standardizes format)
    CASE 
        WHEN LEN(phone) = 9 AND phone LIKE '7%' THEN CONCAT('07', phone)
        WHEN LEN(phone) = 9 AND phone LIKE '1%' THEN CONCAT('01', phone)
        WHEN LEN(phone) = 10 AND phone LIKE '07%' THEN phone
        WHEN LEN(phone) = 12 AND phone LIKE '2547%' THEN CONCAT('0', RIGHT(phone, 9))
        ELSE phone
    END AS phone,
    email,
    county,
    town,
    customer_segment,
    acquisition_channel,
    clean_registration_date AS registration_date,
    clean_churn_date AS churn_date,
    loyalty_tier,
    communication_preferences,
    feedback_score,
    NULL AS home_county,  -- To be updated later
    NULL AS primary_store_id,  -- To be updated later
    CASE 
        WHEN clean_churn_date IS NOT NULL AND clean_churn_date <= GETDATE() THEN 1 
        ELSE 0 
    END AS is_churned,
    DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) AS tenure_days,
    DATEDIFF(MONTH, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) AS tenure_months,
    CASE 
        WHEN DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) < 30 THEN 'New (<30 days)'
        WHEN DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) < 90 THEN 'Recent (30-90 days)'
        WHEN DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) < 180 THEN 'Regular (3-6 months)'
        WHEN DATEDIFF(DAY, clean_registration_date, ISNULL(clean_churn_date, GETDATE())) < 365 THEN 'Established (6-12 months)'
        ELSE 'Loyal (>1 year)'
    END AS tenure_band,
    YEAR(clean_registration_date) AS registration_year,
    MONTH(clean_registration_date) AS registration_month,
    DATEPART(QUARTER, clean_registration_date) AS registration_quarter,
    -- Extract email domain
    CASE 
        WHEN email IS NOT NULL AND CHARINDEX('@', email) > 0 
        THEN RIGHT(email, LEN(email) - CHARINDEX('@', email))
        ELSE NULL 
    END AS email_domain,
    LEFT(phone, 3) AS phone_prefix,
    CASE WHEN LEN(phone) BETWEEN 10 AND 12 AND phone NOT LIKE '%[^0-9]%' THEN 1 ELSE 0 END AS is_phone_valid,
    CASE WHEN email LIKE '%_@__%.__%' THEN 1 ELSE 0 END AS is_email_valid
FROM date_converted
WHERE customer_id IS NOT NULL;





