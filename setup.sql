/*
setup.sql
creates staging table, loads Kaggle CSV data, and builds normalized 3-table schema (customers, loans, defaults)
*/


-- dropping table if already exists so we can run over and over again with a clean base
    -- CASCADE deletes objects that depend on the table 
DROP TABLE IF EXISTS defaults CASCADE;
DROP TABLE IF EXISTS loans CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS credit_risk_staging CASCADE;

-- creating table to load csv - same columns
CREATE TABLE credit_risk_staging (
    -- person_age: how old the person is
    -- each line is one column in the table
    person_age INT,
    
    -- person_income: How much money they make per year
    -- max 12 digits allowed w 2 digits in the decimals
    person_income DECIMAL(12,2),
    
    -- person_home_ownership: do they rent, own, have mortgage, etc.
    -- max 20 characters
    -- ex: "RENT", "OWN", "MORTGAGE", "OTHER"
    person_home_ownership VARCHAR(20),
    
    -- person_emp_length: How long been employed (yrs)
    -- max 5 digits w 2 digits in decimals
    person_emp_length DECIMAL(5,2),
    
     -- loan_intent: reason for loan
    -- max 50 characters
    -- Ex: "EDUCATION", "MEDICAL", "PERSONAL", "HOMEIMPROVEMENT"
    loan_intent VARCHAR(50),

    -- loan_grade: letter grade rating loan quality 
    -- max 5 chars to be safe 
    -- EX: "A", "B", "C", "D", "E", "F", "G" - 
    loan_grade VARCHAR(5),

    -- loan_amnt: How much money they want to borrow
    -- max 12 digits w 2 digits in decimals
    loan_amnt DECIMAL(12,2),
        
    -- loan_int_rate: Interest rate on the loan (percentage)
    -- max 5 digits w 2 decimal places
    loan_int_rate DECIMAL(5,2),
    
    -- loan_status: if loan defaulted or not
    -- TARGET VARIABLE - trying to predict this
    -- only int 
        -- 0 = loan paid back 
        -- 1 = loan defaulted 
    loan_status INT,
    
    -- loan_percent_income: loan amount as a percentage of yearly income
        -- also known as DTI (debt-to-income ratio)
    -- This is also called DTI (Debt-to-Income ratio)
    -- max 5 digits w 4 decimal places
    -- ex: 0.25 -> 25% of income
    loan_percent_income DECIMAL(5,4),
    
    -- cb_person_default_on_file: if person has defaulted before
            -- "cb" -> "credit bureau"
    -- max 1 char
    -- values - 'Y' : defaulted before - 'N' : no history of defaulting
    cb_person_default_on_file VARCHAR(1),
    
    -- cb_person_cred_hist_length: how long person has had credit history (yrs)
    -- only ints
        -- longer credit history = generally more trustworthy
    cb_person_cred_hist_length INT
    
);

\copy credit_risk_staging FROM '/Users/colin/Downloads/SQLproject/credit_risk_dataset.csv' DELIMITER ',' CSV HEADER


-- Verify data loaded correctly
SELECT 
    -- counting total number of rows
    COUNT(*) as total_records,
    -- youngest person in dataset
    MIN(person_age) as min_age,
    -- oldest person in dataset
    MAX(person_age) as max_age,
    -- smallest loan in dataset
    MIN(loan_amnt) as min_loan,
    -- largest loan in dataset
    MAX(loan_amnt) as max_loan,
    -- total # of defaults because all the defaults are set to value of 1
    SUM(loan_status) as total_defaults,
    -- calculating default rate percentage
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate_pct
-- selecting data FROM the credit_risk_staging table after copying data from csv into that table    
FROM credit_risk_staging;

-- trying to see nulls in important columns
SELECT
    'person_age' as column_name,
    -- COUNT(*) = total rows, COUNT(person_age) = non-null rows
    COUNT(*) - COUNT(person_age) as null_count,
    ROUND(100.0 * (COUNT(*) - COUNT(person_age)) / COUNT(*), 2) as null_pct
FROM credit_risk_staging
-- we use UNION ALL because we want all columns 
UNION ALL
SELECT 'person_income', 
       COUNT(*) - COUNT(person_income), 
       ROUND(100.0 * (COUNT(*) - COUNT(person_income)) / COUNT(*), 2)
FROM credit_risk_staging
UNION ALL
SELECT 'person_emp_length', 
       COUNT(*) - COUNT(person_emp_length),
       ROUND(100.0 * (COUNT(*) - COUNT(person_emp_length)) / COUNT(*), 2)
FROM credit_risk_staging
UNION ALL
SELECT 'loan_int_rate', 
       COUNT(*) - COUNT(loan_int_rate),
       ROUND(100.0 * (COUNT(*) - COUNT(loan_int_rate)) / COUNT(*), 2)
FROM credit_risk_staging;


-- Create customers table
CREATE TABLE customers AS
SELECT 
    -- window function - assigning numbers for customer_id to randomly ordered customers
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as customer_id,
    person_age,
    person_income,
    person_home_ownership,
    -- COALESCE: if person_emp_length is null, replace with 0 otherwise use the actual value
    COALESCE(person_emp_length, 0) as person_emp_length,
    cb_person_default_on_file as historical_default,
    cb_person_cred_hist_length as credit_history_length,
    -- I want to add region data to this dataset so we can experiment
    -- and we can analyze different aspects based on region
    -- This basically will generate a random int value from 0-4 
    CASE (RANDOM() * 5)::INT
        WHEN 0 THEN 'Northeast'
        WHEN 1 THEN 'Southeast'
        WHEN 2 THEN 'Midwest'
        WHEN 3 THEN 'Southwest'
        ELSE 'West'
    END as region
FROM credit_risk_staging;

-- creating a primary key makes sure that every customer has a unique id - preventing duplicates
-- also apparently queries are faster with primary keys and we can join tables easier
ALTER TABLE customers ADD PRIMARY KEY (customer_id);

-- making sure we dont have any negative values in our data - would be impossible making data inaccurate
ALTER TABLE customers ADD CONSTRAINT chk_income CHECK (person_income >= 0);
ALTER TABLE customers ADD CONSTRAINT chk_emp_length CHECK (person_emp_length >= 0);

-- ONLY CREATE INDEXES ON COLUMNS WE OFTEN FILTER, JOIN ON, ORDER OR GROUP BY, AND HAS MANY UNIQUE VALUES 
CREATE INDEX idx_customers_income ON customers(person_income);
CREATE INDEX idx_customers_age ON customers(person_age);
CREATE INDEX idx_customers_region ON customers(region);

-- this is info to remind myself 
COMMENT ON TABLE customers IS 'Customer demographic and credit history information';
COMMENT ON COLUMN customers.customer_id IS 'Unique customer identifier (Primary Key)';
COMMENT ON COLUMN customers.historical_default IS 'Y = previous default history, N = no history';
COMMENT ON COLUMN customers.credit_history_length IS 'Length of credit history in years';
