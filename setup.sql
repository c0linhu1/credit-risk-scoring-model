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

-- COPY: Special PostgreSQL command to load data from a file into a table
    -- faster than INSERT statements for larger amounts of data
COPY credit_risk_staging

-- loading rows from csv file 
FROM '/Users/Colin/Downloads/SQLPROJECT/credit_risk_dataset.csv'

-- the csv files separates each character with ','
DELIMITER ','

-- telling PorstgreSQL to skip first row bc its just column names 
CSV HEADER;


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
    COALESCE(person_emp_length, 0) as person_emp_length,
    cb_person_default_on_file as historical_default,
    cb_person_cred_hist_length as credit_history_length,
    CASE (RANDOM() * 5)::INT
        WHEN 0 THEN 'Northeast'
        WHEN 1 THEN 'Southeast'
        WHEN 2 THEN 'Midwest'
        WHEN 3 THEN 'Southwest'
        ELSE 'West'
    END as region
FROM credit_risk_staging;

ALTER TABLE customers ADD PRIMARY KEY (customer_id);
-- Removed age constraint since data has ages > 100
ALTER TABLE customers ADD CONSTRAINT chk_income CHECK (person_income >= 0);
ALTER TABLE customers ADD CONSTRAINT chk_emp_length CHECK (person_emp_length >= 0);

CREATE INDEX idx_customers_income ON customers(person_income);
CREATE INDEX idx_customers_age ON customers(person_age);
CREATE INDEX idx_customers_region ON customers(region);

COMMENT ON TABLE customers IS 'Customer demographic and credit history information';
COMMENT ON COLUMN customers.customer_id IS 'Unique customer identifier (Primary Key)';
COMMENT ON COLUMN customers.historical_default IS 'Y = previous default history, N = no history';
COMMENT ON COLUMN customers.credit_history_length IS 'Length of credit history in years';

-- Create loans table
CREATE TABLE loans AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as loan_id,
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as customer_id,
    loan_amnt,
    loan_intent,
    loan_grade,
    COALESCE(loan_int_rate, 10.0) as loan_int_rate,
    loan_percent_income,
    loan_status,
    DATE '2020-01-01' + (RANDOM() * 1825)::INT as origination_date,
    CASE 
        WHEN loan_amnt < 5000 THEN 12
        WHEN loan_amnt < 10000 THEN 24
        WHEN loan_amnt < 20000 THEN 36
        ELSE 60
    END as loan_term_months,
    ROUND(
        loan_amnt / 
        CASE 
            WHEN loan_amnt < 5000 THEN 12
            WHEN loan_amnt < 10000 THEN 24
            WHEN loan_amnt < 20000 THEN 36
            ELSE 60
        END, 
        2
    ) as monthly_payment
FROM credit_risk_staging;

ALTER TABLE loans ADD PRIMARY KEY (loan_id);
ALTER TABLE loans ADD FOREIGN KEY (customer_id) REFERENCES customers(customer_id);
ALTER TABLE loans ADD CONSTRAINT chk_loan_status CHECK (loan_status IN (0, 1));

CREATE INDEX idx_loans_customer ON loans(customer_id);
CREATE INDEX idx_loans_grade ON loans(loan_grade);
CREATE INDEX idx_loans_date ON loans(origination_date);
CREATE INDEX idx_loans_status ON loans(loan_status);
CREATE INDEX idx_loans_intent ON loans(loan_intent);

COMMENT ON TABLE loans IS 'Loan application details, terms, and status';
COMMENT ON COLUMN loans.loan_id IS 'Unique loan identifier (Primary Key)';
COMMENT ON COLUMN loans.loan_status IS '0 = current/paid, 1 = defaulted';
COMMENT ON COLUMN loans.loan_percent_income IS 'Loan amount as percentage of annual income (debt-to-income ratio)';
COMMENT ON COLUMN loans.origination_date IS 'Date loan was originated';

-- Create defaults table (fixed ROUND casting issue)
CREATE TABLE defaults AS
SELECT 
    l.loan_id,
    l.customer_id,
    l.origination_date + (
        (l.loan_term_months * (0.3 + RANDOM() * 0.4))::INT || ' months'
    )::INTERVAL as default_date,
    ROUND((l.loan_amnt * (0.4 + RANDOM() * 0.4))::NUMERIC, 2) as outstanding_balance,
    ROUND((l.loan_amnt * (0.4 + RANDOM() * 0.4) * (0.2 + RANDOM() * 0.3))::NUMERIC, 2) as recovered_amount,
    CASE 
        WHEN RANDOM() < 0.3 THEN 'IN_COLLECTION'
        WHEN RANDOM() < 0.6 THEN 'PARTIALLY_RECOVERED'
        ELSE 'CHARGED_OFF'
    END as recovery_status
FROM loans l
WHERE l.loan_status = 1;

ALTER TABLE defaults ADD PRIMARY KEY (loan_id);
ALTER TABLE defaults ADD FOREIGN KEY (loan_id) REFERENCES loans(loan_id);
ALTER TABLE defaults ADD FOREIGN KEY (customer_id) REFERENCES customers(customer_id);

CREATE INDEX idx_defaults_date ON defaults(default_date);
CREATE INDEX idx_defaults_status ON defaults(recovery_status);

COMMENT ON TABLE defaults IS 'Default events and recovery information for defaulted loans';
COMMENT ON COLUMN defaults.outstanding_balance IS 'Remaining balance at time of default';
COMMENT ON COLUMN defaults.recovered_amount IS 'Amount recovered through collections';

-- Create summary view
CREATE OR REPLACE VIEW vw_loan_summary AS
SELECT 
    l.loan_id,
    l.customer_id,
    c.person_age,
    c.person_income,
    c.person_home_ownership,
    c.person_emp_length,
    c.credit_history_length,
    c.historical_default,
    c.region,
    l.loan_amnt,
    l.loan_intent,
    l.loan_grade,
    l.loan_int_rate,
    l.loan_percent_income,
    l.loan_status,
    l.origination_date,
    l.loan_term_months,
    l.monthly_payment,
    d.default_date,
    d.outstanding_balance,
    d.recovered_amount,
    d.recovery_status,
    CASE 
        WHEN l.loan_status = 1 AND d.default_date IS NOT NULL 
        THEN EXTRACT(MONTH FROM AGE(d.default_date, l.origination_date))
        ELSE EXTRACT(MONTH FROM AGE(CURRENT_DATE, l.origination_date))
    END as loan_age_months
FROM loans l
JOIN customers c ON l.customer_id = c.customer_id
LEFT JOIN defaults d ON l.loan_id = d.loan_id;

COMMENT ON VIEW vw_loan_summary IS 'Denormalized view combining loan, customer, and default data';

-- Verification queries
SELECT 
    'STAGING' as table_name, 
    COUNT(*) as row_count,
    'Raw CSV data' as description
FROM credit_risk_staging
UNION ALL
SELECT 'CUSTOMERS', COUNT(*), 'Unique customer records'
FROM customers
UNION ALL
SELECT 'LOANS', COUNT(*), 'All loan records'
FROM loans
UNION ALL
SELECT 'DEFAULTS', COUNT(*), 'Defaulted loans only'
FROM defaults;

SELECT 
    'Orphaned Loans' as integrity_check,
    COUNT(*) as issue_count
FROM loans l
LEFT JOIN customers c ON l.customer_id = c.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 
    'Loans with status=1 missing default record',
    COUNT(*)
FROM loans l
LEFT JOIN defaults d ON l.loan_id = d.loan_id
WHERE l.loan_status = 1 AND d.loan_id IS NULL
UNION ALL
SELECT 
    'Defaults without matching loan',
    COUNT(*)
FROM defaults d
LEFT JOIN loans l ON d.loan_id = l.loan_id
WHERE l.loan_id IS NULL;

SELECT 
    loan_grade,
    COUNT(*) as loan_count,
    ROUND(AVG(loan_amnt), 0) as avg_loan_amount,
    ROUND(AVG(loan_int_rate), 2) as avg_interest_rate,
    SUM(loan_status) as total_defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate_pct
FROM loans
GROUP BY loan_grade
ORDER BY loan_grade;

SELECT 
    COUNT(*) as total_loans,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(SUM(loan_amnt), 0) as total_loan_volume,
    ROUND(AVG(loan_amnt), 0) as avg_loan_size,
    SUM(loan_status) as total_defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as overall_default_rate,
    ROUND(AVG(loan_int_rate), 2) as avg_interest_rate
FROM loans;