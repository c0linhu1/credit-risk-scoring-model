/*
setup.sql
creates staging table, loads Kaggle CSV data, and builds table 'loans'
*/


-- dropping table if already exists so we can run over and over again with a clean base
    -- CASCADE deletes objects that depend on the table 
DROP TABLE IF EXISTS loans CASCADE;


-- creating table to load csv - same columns
CREATE TABLE loans (
    -- person_age: how old the person is
    -- each line is one column in the table
    person_age INT,
    
    -- person_income: How much money they make per year
    person_income INT,
    
    -- person_home_ownership: do they rent, own, have mortgage, etc.
    -- max 20 characters
    -- ex: "RENT", "OWN", "MORTGAGE", "OTHER"
    person_home_ownership VARCHAR(20),
    
    -- person_emp_length: How long been employed (yrs)
    -- max 5 digits w 2 digits in decimals
    person_emp_length DECIMAL(5,2),
    
     -- loan_intent: reason for loan
    -- ['DEBTCONSOLIDATION', 'EDUCATION', 'HOMEIMPROVEMENT', 'MEDICAL', 'PERSONAL', 'VENTURE']
    loan_intent VARCHAR(30),

    -- loan_grade: letter grade rating loan quality 
    -- ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    loan_grade VARCHAR(1),

    -- loan_amnt: How much money they want to borrow
    loan_amnt INT,
        
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
    -- max 4 digits w 2 decimal places
    -- ex: 0.25 -> 25% of income
    loan_percent_income DECIMAL(3,2),
    
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

\copy loans FROM '/Users/colin/Downloads/SQLproject/credit_risk_dataset.csv' DELIMITER ',' CSV HEADER


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
-- selecting data FROM the loans table after copying data from csv into that table    
FROM loans;

-- trying to see nulls in important columns
SELECT
    'person_age' as column_name,
    -- COUNT(*) = total rows, COUNT(person_age) = non-null rows
    COUNT(*) - COUNT(person_age) as null_count,
    ROUND(100.0 * (COUNT(*) - COUNT(person_age)) / COUNT(*), 2) as null_pct
FROM loans
-- we use UNION ALL because we want all columns 
UNION ALL
SELECT 'person_income', 
       COUNT(*) - COUNT(person_income), 
       ROUND(100.0 * (COUNT(*) - COUNT(person_income)) / COUNT(*), 2)
FROM loans
UNION ALL
SELECT 'person_emp_length', 
       COUNT(*) - COUNT(person_emp_length),
       ROUND(100.0 * (COUNT(*) - COUNT(person_emp_length)) / COUNT(*), 2)
FROM loans
UNION ALL
SELECT 'loan_int_rate', 
       COUNT(*) - COUNT(loan_int_rate),
       ROUND(100.0 * (COUNT(*) - COUNT(loan_int_rate)) / COUNT(*), 2)
FROM loans;

--SELECT * FROM loans LIMIT 10;