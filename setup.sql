/*
setup.sql
creates staging table, loads Kaggle CSV data, and builds normalized 3-table schema (customers, loans, defaults)
*/


-- dropping table if already exists so we can run over and over again with a clean base
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
    
    -- loan_amnt: How much money they want to borrow
    -- max 12 digits w 2 digits in decimals
    loan_amnt DECIMAL(12,2),
    
    -- loan_intent: reason for loan
    -- max 50 characters
    -- Ex: "EDUCATION", "MEDICAL", "PERSONAL", "HOMEIMPROVEMENT"
    loan_intent VARCHAR(50),
    
    -- loan_grade: letter grade rating loan quality 
    -- max 5 chars to be safe 
    -- EX: "A", "B", "C", "D", "E", "F", "G" - 
    loan_grade VARCHAR(5),
    
    -- loan_int_rate: Interest rate on the loan (percentage)
    -- max 5 digits w 2 decimal places
    loan_int_rate DECIMAL(5,2),
    
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
    cb_person_cred_hist_length INT,
    
    -- loan_status: if loan defaulted or not
    -- TARGET VARIABLE - trying to predict this
    -- only int 
        -- 0 = loan paid back 
        -- 1 = loan defaulted 
    loan_status INT
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

# verification we copied data correctly 
SELECT 
    -- COUNT(*): How many rows are in the table?
    -- The * means "count everything"
    -- "as total_records": Renames the column in the output to "total_records"
    COUNT(*) as total_records,
    
    -- MIN(): Finds the smallest (minimum) value in a column
    -- What's the youngest person in our dataset?
    MIN(person_age) as min_age,
    
    -- MAX(): Finds the largest (maximum) value in a column
    -- What's the oldest person in our dataset?
    MAX(person_age) as max_age,
    
    -- Smallest loan amount in the dataset
    MIN(loan_amnt) as min_loan,
    
    -- Largest loan amount in the dataset
    MAX(loan_amnt) as max_loan,
    
    -- SUM(): Adds up all the values in a column
    -- Since loan_status is 0 or 1, adding them up = counting the 1s
    -- This tells us how many loans defaulted
    SUM(loan_status) as total_defaults,
    
    -- Calculate the default rate as a percentage
    -- Formula: (number of defaults / total loans) * 100
    -- 
    -- Why "100.0" instead of "100"?
    -- In SQL, 100 is an integer, so 100 / 200 = 0 (integer division)
    -- But 100.0 is a decimal, so 100.0 / 200 = 0.5 (decimal division)
    -- 
    -- SUM(loan_status) = number of defaults
    -- COUNT(*) = total number of loans
    -- SUM(loan_status) / COUNT(*) = percentage as decimal (0.18)
    -- Multiply by 100 to get percentage (18%)
    -- 
    -- ROUND(..., 2): Rounds the number to 2 decimal places
    -- Example: 18.4567 becomes 18.46
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate_pct
-- FROM: Which table are we getting this data from?
FROM credit_risk_staging;
-- After running this, you should see something like:
-- total_records: ~32,000
-- min_age: 20
-- max_age: 144 (this seems wrong - data quality issue!)
-- min_loan: 500
-- max_loan: 35,000
-- total_defaults: ~7,000
-- default_rate_pct: ~21%

-- Now let's check for missing data (NULLs)
-- NULL means "no value" or "missing data" - different from 0 or empty text
-- Missing data can cause problems in analysis, so we need to know about it
SELECT 
    -- This is a text label - we're creating it ourselves
    -- It will show up as the column name in results
    'person_age' as column_name,
    
    -- How many NULLs are in the person_age column?
    -- COUNT(*) = total rows
    -- COUNT(person_age) = rows where person_age is NOT NULL
    -- The difference = rows where person_age IS NULL
    COUNT(*) - COUNT(person_age) as null_count,
    
    -- Calculate what percentage of values are missing
    -- Same concept as above, but as a percentage
    ROUND(100.0 * (COUNT(*) - COUNT(person_age)) / COUNT(*), 2) as null_pct
FROM credit_risk_staging

-- UNION ALL: Combines results from multiple SELECT statements
-- This lets us check multiple columns in one output
-- 
-- UNION vs UNION ALL:
-- - UNION removes duplicate rows (slower)
-- - UNION ALL keeps all rows (faster)
-- Since we know these won't have duplicates, we use UNION ALL
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
-- After running this, you'll see which columns have missing data
-- Typical results:
-- person_emp_length: ~10% missing (some people don't report employment)
-- loan_int_rate: ~2% missing (some loans don't have rates recorded)
-- We'll handle these NULLs later with COALESCE

-- COMMENT ON: This adds documentation to database objects
-- It's like a permanent comment stored IN the database
-- Anyone who looks at this table later can see what it's for
-- This is good practice for professional projects!
COMMENT ON TABLE credit_risk_staging IS 'Staging table for raw Kaggle credit risk dataset';


-- =============================================================================
-- STEP 2: CREATE CUSTOMERS TABLE (NORMALIZED CUSTOMER DATA)
-- =============================================================================

-- Now we're creating the REAL tables for analysis
-- Why separate tables instead of one big table?
-- 1. Normalization: Avoid repeating customer data for each loan
-- 2. Performance: Faster queries with proper structure
-- 3. Integrity: Changes to customer info update everywhere automatically

-- CREATE TABLE ... AS SELECT: Creates a table AND fills it with data in one step
-- This is different from CREATE TABLE (which just makes an empty structure)
CREATE TABLE customers AS
SELECT 
    -- ROW_NUMBER(): Assigns a sequential number to each row
    -- OVER (ORDER BY RANDOM()): The order is random (shuffled)
    -- This creates a unique ID for each customer: 1, 2, 3, 4, ...
    -- 
    -- Why ORDER BY RANDOM()?
    -- So customer IDs are assigned randomly, not based on age or income
    -- This prevents any unintentional patterns in the data
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as customer_id,
    
    -- These columns we're copying directly - no changes
    person_age,
    person_income,
    person_home_ownership,
    
    -- COALESCE(value1, value2, value3, ...):
    -- Returns the first non-NULL value in the list
    -- 
    -- COALESCE(person_emp_length, 0):
    -- - If person_emp_length is NOT NULL, use that value
    -- - If person_emp_length IS NULL, use 0 instead
    -- 
    -- Why do this?
    -- NULLs cause problems in calculations (NULL + 5 = NULL)
    -- It's safer to replace NULLs with a sensible default
    -- For employment length, 0 means "newly employed" which is reasonable
    COALESCE(person_emp_length, 0) as person_emp_length,
    
    -- Renaming columns to be more descriptive
    -- "cb_person_default_on_file" is confusing
    -- "historical_default" is much clearer!
    cb_person_default_on_file as historical_default,
    
    -- Same thing - making the name more readable
    cb_person_cred_hist_length as credit_history_length,
    
    -- CASE: This is SQL's if-then-else logic
    -- Format:
    --   CASE expression
    --     WHEN condition1 THEN result1
    --     WHEN condition2 THEN result2
    --     ELSE default_result
    --   END
    -- 
    -- Let's break down what's happening here:
    -- 
    -- RANDOM(): Generates a random number between 0 and 1
    -- Examples: 0.1538, 0.7234, 0.9912
    -- 
    -- RANDOM() * 5: Multiplies by 5, giving 0 to 5
    -- Examples: 0.7690, 3.6170, 4.9560
    -- 
    -- (RANDOM() * 5)::INT: Converts to integer (truncates decimal)
    -- ::INT is PostgreSQL's way of converting (casting) data types
    -- Examples: 0, 3, 4
    -- 
    -- So we get a random integer: 0, 1, 2, 3, or 4
    -- 
    -- Then CASE checks which number we got:
    CASE (RANDOM() * 5)::INT
        WHEN 0 THEN 'Northeast'  -- If we got 0, region is Northeast
        WHEN 1 THEN 'Southeast'  -- If we got 1, region is Southeast
        WHEN 2 THEN 'Midwest'    -- If we got 2, region is Midwest
        WHEN 3 THEN 'Southwest'  -- If we got 3, region is Southwest
        ELSE 'West'              -- If we got 4, region is West
    END as region
    -- Why generate fake regions?
    -- The original dataset doesn't have location data
    -- But location-based analysis is important for risk assessment
    -- This lets us demonstrate geographic analysis in our project
    -- In a real project, you'd have actual location data!
-- FROM: Where is this data coming from?
FROM credit_risk_staging;
-- This SELECT runs once for each row in credit_risk_staging
-- So if staging has 32,000 rows, customers will have 32,000 rows

-- ALTER TABLE: Changes the structure of an existing table
-- We use this to add constraints AFTER the table is created
-- (You can't add them during CREATE TABLE ... AS SELECT)

-- ADD PRIMARY KEY: Marks a column as the unique identifier
-- Rules for primary keys:
-- 1. Must be unique (no two rows can have the same value)
-- 2. Cannot be NULL (must always have a value)
-- 3. Each table should have exactly one primary key
-- 
-- Why do we need primary keys?
-- - Uniquely identifies each row
-- - Makes queries faster (automatically creates an index)
-- - Allows other tables to reference this table (foreign keys)
ALTER TABLE customers ADD PRIMARY KEY (customer_id);

-- ADD CONSTRAINT: Adds a rule that data must follow
-- constraint_name: "chk_age" (chk = check, good naming convention)
-- 
-- CHECK: Creates a validation rule
-- Every row must satisfy this condition, or PostgreSQL rejects it
-- 
-- This ensures age is between 18 and 100
-- Why? 
-- - Can't give loans to minors (< 18)
-- - 100+ seems like a data error (we saw 144 in our data - probably wrong!)
ALTER TABLE customers ADD CONSTRAINT chk_age 
    CHECK (person_age >= 18 AND person_age <= 100);

-- Income must be 0 or positive
-- Negative income doesn't make sense
ALTER TABLE customers ADD CONSTRAINT chk_income 
    CHECK (person_income >= 0);

-- Employment length must be 0 or positive
-- Negative years of employment is impossible
ALTER TABLE customers ADD CONSTRAINT chk_emp_length 
    CHECK (person_emp_length >= 0);

-- CREATE INDEX: Creates a "lookup table" for faster queries
-- Think of an index like the index in the back of a textbook
-- Instead of scanning every page, you can jump to the right section
-- 
-- Format: CREATE INDEX index_name ON table_name(column_name)
-- 
-- Naming convention: idx_tablename_columnname
-- 
-- When should you create an index?
-- 1. Columns used in WHERE clauses (filtering)
-- 2. Columns used in JOIN conditions
-- 3. Columns used in ORDER BY (sorting)
-- 
-- When should you NOT create an index?
-- - Columns you never query by
-- - Tables with very few rows (< 1000)
-- - Columns that change frequently (indexes slow down INSERT/UPDATE)

-- Index on income: We'll filter by income ranges in analysis
CREATE INDEX idx_customers_income ON customers(person_income);

-- Index on age: We'll group by age ranges
CREATE INDEX idx_customers_age ON customers(person_age);

-- Index on region: We'll filter and group by region
CREATE INDEX idx_customers_region ON customers(region);

-- Add documentation comments
-- These are visible in database tools like pgAdmin
-- Very professional - shows you care about maintainability!
COMMENT ON TABLE customers IS 'Customer demographic and credit history information';
COMMENT ON COLUMN customers.customer_id IS 'Unique customer identifier (Primary Key)';
COMMENT ON COLUMN customers.historical_default IS 'Y = previous default history, N = no history';
COMMENT ON COLUMN customers.credit_history_length IS 'Length of credit history in years';


-- =============================================================================
-- STEP 3: CREATE LOANS TABLE (LOAN DETAILS AND TERMS)
-- =============================================================================

-- The loans table connects customers to their loans
-- One customer can have multiple loans (one-to-many relationship)

CREATE TABLE loans AS
SELECT 
    -- Create unique loan ID
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as loan_id,
    
    -- customer_id comes from the customers table we just created
    -- This is what links each loan to a customer
    -- Later we'll make this a FOREIGN KEY
    customer_id,
    
    -- Copy loan details directly
    loan_amnt,
    loan_intent,
    loan_grade,
    
    -- Handle NULL interest rates by using 10.0% as default
    -- Why 10.0? It's a reasonable middle-ground rate
    COALESCE(loan_int_rate, 10.0) as loan_int_rate,
    
    loan_percent_income,
    loan_status,
    
    -- Generate realistic loan origination dates
    -- The original data doesn't have dates - we need to create them
    -- 
    -- DATE '2020-01-01': Starting date (January 1, 2020)
    -- RANDOM() * 1825: Random number from 0 to 1825
    --   Why 1825? That's 5 years in days (365 * 5 = 1825)
    -- ::INT: Convert to integer (whole days)
    -- 
    -- So this generates random dates between:
    -- 2020-01-01 and 2024-12-31 (approximately)
    -- 
    -- Examples:
    -- - RANDOM() = 0.0 → 2020-01-01 + 0 days = 2020-01-01
    -- - RANDOM() = 0.5 → 2020-01-01 + 912 days = 2022-06-24
    -- - RANDOM() = 1.0 → 2020-01-01 + 1825 days = 2024-12-31
    DATE '2020-01-01' + (RANDOM() * 1825)::INT as origination_date,
    
    -- Calculate loan term based on loan amount
    -- This follows real-world lending patterns:
    -- - Small loans: Short term (can't charge much interest, pay off quickly)
    -- - Large loans: Long term (more interest, need time to repay)
    CASE 
        WHEN loan_amnt < 5000 THEN 12    -- Under $5,000: 12 months (1 year)
        WHEN loan_amnt < 10000 THEN 24   -- $5,000-$9,999: 24 months (2 years)
        WHEN loan_amnt < 20000 THEN 36   -- $10,000-$19,999: 36 months (3 years)
        ELSE 60                          -- $20,000+: 60 months (5 years)
    END as loan_term_months,
    
    -- Calculate monthly payment amount
    -- Simple formula: Total loan amount ÷ Number of months
    -- (In reality, interest makes this more complex, but this is fine for our project)
    -- 
    -- We need to repeat the CASE logic because we can't reference
    -- loan_term_months in the same SELECT (it doesn't exist yet!)
    ROUND(
        loan_amnt /   -- Divide loan amount by term
        CASE 
            WHEN loan_amnt < 5000 THEN 12
            WHEN loan_amnt < 10000 THEN 24
            WHEN loan_amnt < 20000 THEN 36
            ELSE 60
        END, 
        2  -- Round to 2 decimal places (dollars and cents)
    ) as monthly_payment
    -- Example: $10,000 loan over 24 months = $10,000 / 24 = $416.67/month
FROM customers;
-- We're pulling from customers because customers was created from
-- credit_risk_staging, so it has all the loan columns too

-- Add primary key to uniquely identify each loan
ALTER TABLE loans ADD PRIMARY KEY (loan_id);

-- FOREIGN KEY: Creates a relationship between two tables
-- This creates a link from loans.customer_id to customers.customer_id
-- 
-- What does this do?
-- 1. Prevents "orphaned" loans (loans with no matching customer)
-- 2. Can set up CASCADE delete (if customer deleted, delete their loans)
-- 3. Helps database optimize join queries
-- 
-- Format: FOREIGN KEY (column_in_this_table) 
--         REFERENCES other_table(column_in_other_table)
ALTER TABLE loans ADD FOREIGN KEY (customer_id) 
    REFERENCES customers(customer_id);

-- Ensure loan_status is only 0 or 1
-- IN (0, 1) means "must be in this list"
-- This prevents typos like loan_status = 2 or loan_status = 'defaulted'
ALTER TABLE loans ADD CONSTRAINT chk_loan_status 
    CHECK (loan_status IN (0, 1));

-- Create indexes for common query patterns
-- customer_id: We'll JOIN on this constantly
CREATE INDEX idx_loans_customer ON loans(customer_id);

-- loan_grade: We'll analyze performance by grade
CREATE INDEX idx_loans_grade ON loans(loan_grade);

-- origination_date: We'll do cohort analysis by date
CREATE INDEX idx_loans_date ON loans(origination_date);

-- loan_status: We'll filter to defaulted vs non-defaulted loans
CREATE INDEX idx_loans_status ON loans(loan_status);

-- loan_intent: We'll analyze which loan purposes are riskiest
CREATE INDEX idx_loans_intent ON loans(loan_intent);

-- Add documentation
COMMENT ON TABLE loans IS 'Loan application details, terms, and status';
COMMENT ON COLUMN loans.loan_id IS 'Unique loan identifier (Primary Key)';
COMMENT ON COLUMN loans.loan_status IS '0 = current/paid, 1 = defaulted';
COMMENT ON COLUMN loans.loan_percent_income IS 'Loan amount as percentage of annual income (debt-to-income ratio)';
COMMENT ON COLUMN loans.origination_date IS 'Date loan was originated';


-- =============================================================================
-- STEP 4: CREATE DEFAULTS TABLE (DEFAULT EVENTS AND RECOVERY INFO)
-- =============================================================================

-- This table contains ONLY defaulted loans
-- It's separate because:
-- 1. Most loans don't default (80%), so most rows wouldn't need this data
-- 2. Default-specific info (recovery amounts, dates) only applies to defaults
-- 3. Keeps loans table cleaner and queries faster

CREATE TABLE defaults AS
SELECT 
    -- l.loan_id: The "l." means "from the loans table"
    -- We give tables aliases (short names) to make code cleaner
    -- "loans l" means "call the loans table 'l' for this query"
    l.loan_id,
    l.customer_id,
    
    -- Calculate when the default occurred
    -- Defaults don't happen immediately or at the very end
    -- They typically happen partway through the loan term
    -- 
    -- Step by step:
    -- 1. Start with origination_date (when loan started)
    -- 2. Calculate what % through the loan term they default
    --    - (0.3 + RANDOM() * 0.4): Random value between 0.3 and 0.7
    --    - This means defaults happen 30-70% through the loan term
    --    - Example: For 24-month loan, default happens month 7-17
    -- 3. Multiply by loan_term_months to get actual months
    --    - If loan term is 24 months and random = 0.5
    --    - 24 * (0.3 + 0.5 * 0.4) = 24 * 0.5 = 12 months
    -- 4. ::INT converts to whole number
    -- 5. || ' months' adds text to make "12 months"
    -- 6. ::INTERVAL converts to a time interval PostgreSQL can add to dates
    -- 7. Add this interval to origination_date
    -- 
    -- Example: 
    -- Loan started 2020-01-01, term is 24 months, random factor = 0.5
    -- Default date = 2020-01-01 + 12 months = 2020-12-01
    l.origination_date + (
        (l.loan_term_months * (0.3 + RANDOM() * 0.4))::INT || ' months'
    )::INTERVAL as default_date,
    
    -- Outstanding balance: How much is still owed when they default?
    -- People don't default immediately (they make some payments first)
    -- So outstanding balance is 40-80% of original loan amount
    -- 
    -- (0.4 + RANDOM() * 0.4):
    -- - Minimum: 0.4 (40% of loan still owed)
    -- - Maximum: 0.8 (80% of loan still owed)
    -- - Average: 0.6 (60% of loan still owed)
    -- 
    -- Example: $10,000 loan
    -- - If random = 0, outstanding = $10,000 * 0.4 = $4,000
    -- - If random = 0.5, outstanding = $10,000 * 0.6 = $6,000
    -- - If random = 1, outstanding = $10,000 * 0.8 = $8,000
    ROUND(l.loan_amnt * (0.4 + RANDOM() * 0.4), 2) as outstanding_balance,
    
    -- Recovered amount: How much did we get back through collections?
    -- Recovery rates are typically low (20-50% of outstanding balance)
    -- 
    -- This is a two-step calculation:
    -- 1. First calculate outstanding balance (same as above)
    -- 2. Then multiply by recovery rate (0.2 to 0.5)
    -- 
    -- (0.2 + RANDOM() * 0.3):
    -- - Minimum: 0.2 (recover 20% of outstanding)
    -- - Maximum: 0.5 (recover 50% of outstanding)
    -- - Average: 0.35 (recover 35% of outstanding)
    -- 
    -- Example: $10,000 loan, $6,000 outstanding
    -- - If recovery = 0.2, recovered = $6,000 * 0.2 = $1,200
    -- - If recovery = 0.35, recovered = $6,000 * 0.35 = $2,100
    -- - If recovery = 0.5, recovered = $6,000 * 0.5 = $3,000
    ROUND(
        l.loan_amnt * (0.4 + RANDOM() * 0.4) * (0.2 + RANDOM() * 0.3), 2
    ) as recovered_amount,
    
    -- Recovery status: What's the current state of recovery efforts?
    -- 
    -- RANDOM() < 0.3 means "30% of the time this is true"
    -- 
    -- Breakdown:
    -- - 30% chance: IN_COLLECTION (still trying to collect)
    -- - 30% chance: PARTIALLY_RECOVERED (got some money back)
    -- - 40% chance: CHARGED_OFF (gave up, wrote off the loss)
    -- 
    -- How does this work?
    -- - RANDOM() generates 0.0 to 1.0
    -- - If RANDOM() = 0.15 (< 0.3), then IN_COLLECTION
    -- - If RANDOM() = 0.45 (< 0.6 but not < 0.3), then PARTIALLY_RECOVERED
    -- - If RANDOM() = 0.75 (not < 0.6), then CHARGED_OFF
    CASE 
        WHEN RANDOM() < 0.3 THEN 'IN_COLLECTION'
        WHEN RANDOM() < 0.6 THEN 'PARTIALLY_RECOVERED'  -- This is 0.3-0.6 (30%)
        ELSE 'CHARGED_OFF'                               -- This is 0.6-1.0 (40%)
    END as recovery_status
-- FROM: Get data from the loans table
-- "loans l" means "call it 'l' as a shorthand"
FROM loans l
-- WHERE: Filter to only include certain rows
-- We only want loans where loan_status = 1 (defaulted loans)
-- loan_status = 0 means the loan was paid back successfully
-- 
-- Why this matters:
-- If we have 32,000 loans and 20% default rate:
-- - 32,000 * 0.20 = 6,400 rows in defaults table
-- - The other 25,600 "good" loans won't have default records
WHERE l.loan_status = 1;

-- Add primary key
-- loan_id uniquely identifies each default
-- Note: loan_id here is BOTH primary key AND foreign key
ALTER TABLE defaults ADD PRIMARY KEY (loan_id);

-- Add foreign keys to maintain relationships
-- This ensures every default record has a matching loan
ALTER TABLE defaults ADD FOREIGN KEY (loan_id) 
    REFERENCES loans(loan_id);

-- This ensures every default record has a matching customer
ALTER TABLE defaults ADD FOREIGN KEY (customer_id) 
    REFERENCES customers(customer_id);

-- Create indexes for queries
CREATE INDEX idx_defaults_date ON defaults(default_date);
CREATE INDEX idx_defaults_status ON defaults(recovery_status);

-- Documentation
COMMENT ON TABLE defaults IS 'Default events and recovery information for defaulted loans';
COMMENT ON COLUMN defaults.outstanding_balance IS 'Remaining balance at time of default';
COMMENT ON COLUMN defaults.recovered_amount IS 'Amount recovered through collections';


-- =============================================================================
-- STEP 5: CREATE USEFUL VIEWS FOR ANALYSIS
-- =============================================================================

-- VIEW: A saved query that acts like a virtual table
-- You can SELECT from a view just like a regular table
-- But it doesn't store data - it runs the query each time
-- 
-- Why use views?
-- 1. Simplify complex queries - write once, use many times
-- 2. Hide complexity from end users
-- 3. Provide consistent business logic
-- 4. Security - can give people access to view without raw tables
-- 
-- OR REPLACE: If view already exists, replace it with this new definition
CREATE OR REPLACE VIEW vw_loan_summary AS
SELECT 
    -- Pull columns from all three tables
    -- Prefix with table alias to show where each comes from
    l.loan_id,
    l.customer_id,
    
    -- Customer demographics
    c.person_age,
    c.person_income,
    c.person_home_ownership,
    c.person_emp_length,
    c.credit_history_length,
    c.historical_default,
    c.region,
    
    -- Loan details
    l.loan_amnt,
    l.loan_intent,
    l.loan_grade,
    l.loan_int_rate,
    l.loan_percent_income,
    l.loan_status,
    l.origination_date,
    l.loan_term_months,
    l.monthly_payment,
    
    -- Default information (will be NULL for non-defaulted loans)
    -- d.default_date will be NULL if loan didn't default
    d.default_date,
    d.outstanding_balance,
    d.recovered_amount,
    d.recovery_status,
    
    -- Calculate how old the loan is (in months)
    -- Different calculation for defaulted vs current loans
    CASE 
        -- For defaulted loans: measure from origination to default
        WHEN l.loan_status = 1 AND d.default_date IS NOT NULL 
        THEN EXTRACT(MONTH FROM AGE(d.default_date, l.origination_date))
        -- For current loans: measure from origination to today
        -- CURRENT_DATE is today's date
        ELSE EXTRACT(MONTH FROM AGE(CURRENT_DATE, l.origination_date))
    END as loan_age_months
    -- 
    -- AGE(date1, date2): Calculates time between two dates
    -- EXTRACT(MONTH FROM interval): Gets the months part
    -- 
    -- Example:
    -- - Origination: 2020-01-01
    -- - Default: 2021-01-01
    -- - AGE(2021-01-01, 2020-01-01) = 1 year
    -- - EXTRACT(MONTH FROM 1 year) = 12 months
-- FROM: Start with loans table
FROM loans l
-- JOIN: Combine loans with customers
-- INNER JOIN (default JOIN): Only include rows that match in both tables
-- Every loan should have a customer, so this is fine
-- 
-- ON l.customer_id = c.customer_id:
-- The join condition - how do we match rows?
-- Find rows where customer_id is the same in both tables
JOIN customers c ON l.customer_id = c.customer_id
-- LEFT JOIN: Include all rows from left table (loans)
-- even if there's no match in right table (defaults)
-- 
-- Why LEFT JOIN instead of JOIN?
-- - Most loans (80%) don't default
-- - Those loans won't have a record in defaults table
-- - JOIN would exclude them (we'd only see defaulted loans)
-- - LEFT JOIN includes them with NULL default columns
-- 
-- Result:
-- - For non-defaulted loans: default columns are NULL
-- - For defaulted loans: default columns have values
LEFT JOIN defaults d ON l.loan_id = d.loan_id;

-- Add documentation
COMMENT ON VIEW vw_loan_summary IS 'Denormalized view combining loan, customer, and default data';
-- 
-- This view is "denormalized" - it combines normalized tables back together
-- Normalized = data split across multiple tables (customers, loans, defaults)
-- Denormalized = everything in one view (easier to query)
-- 
-- Use this view when you want all information in one query!


-- =============================================================================
-- STEP 6: FINAL VERIFICATION AND SUMMARY STATISTICS
-- =============================================================================

-- Let's verify everything worked correctly!

-- Check 1: How many rows in each table?
-- This helps ensure data loaded properly
SELECT 
    -- These are literal text values (in quotes)
    -- They become column values in the result
    'STAGING' as table_name, 
    COUNT(*) as row_count,
    'Raw CSV data' as description
FROM credit_risk_staging

-- UNION ALL: Stack results vertically (rows below rows)
-- Each SELECT must have the same number and type of columns
UNION ALL
SELECT 'CUSTOMERS', COUNT(*), 'Unique customer records'
FROM customers

UNION ALL
SELECT 'LOANS', COUNT(*), 'All loan records'
FROM loans

UNION ALL
SELECT 'DEFAULTS', COUNT(*), 'Defaulted loans only'
FROM defaults;
-- 
-- Expected results:
-- STAGING: ~32,000 rows
-- CUSTOMERS: ~32,000 rows (same as staging)
-- LOANS: ~32,000 rows (one loan per staging record)
-- DEFAULTS: ~6,400 rows (20% of loans)

-- Check 2: Verify referential integrity
-- "Referential integrity" means relationships between tables are correct
-- We're checking for "orphaned" records (records that should link but don't)
SELECT 
    'Orphaned Loans' as integrity_check,
    COUNT(*) as issue_count
FROM loans l
-- LEFT JOIN: Include all loans, even if no matching customer
LEFT JOIN customers c ON l.customer_id = c.customer_id
-- WHERE c.customer_id IS NULL: Find loans with no matching customer
-- This should be 0! If it's not, something went wrong.
WHERE c.customer_id IS NULL

UNION ALL
SELECT 
    'Loans with status=1 missing default record',
    COUNT(*)
FROM loans l
LEFT JOIN defaults d ON l.loan_id = d.loan_id
-- Find defaulted loans (status=1) that don't have a default record
-- This should also be 0!
WHERE l.loan_status = 1 AND d.loan_id IS NULL

UNION ALL
SELECT 
    'Defaults without matching loan',
    COUNT(*)
FROM defaults d
LEFT JOIN loans l ON d.loan_id = l.loan_id
-- Find default records that don't have a matching loan
-- This should definitely be 0!
WHERE l.loan_id IS NULL;
-- 
-- If all three checks show 0, our data integrity is perfect!
-- If any show > 0, we have a problem to investigate

-- Check 3: Summary statistics by loan grade
-- This gives us a preview of what we'll analyze later
SELECT 
    loan_grade,
    -- How many loans in each grade?
    COUNT(*) as loan_count,
    -- Average loan amount for this grade
    ROUND(AVG(loan_amnt), 0) as avg_loan_amount,
    -- Average interest rate for this grade
    ROUND(AVG(loan_int_rate), 2) as avg_interest_rate,
    -- How many defaults in this grade?
    SUM(loan_status) as total_defaults,
    -- What's the default rate for this grade?
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate_pct
FROM loans
-- GROUP BY: Calculate separate statistics for each loan_grade
-- Without GROUP BY, we'd get overall statistics for all loans
-- With GROUP BY, we get one row per grade
GROUP BY loan_grade
-- ORDER BY: Sort the results
-- This sorts alphabetically: A, B, C, D, E, F, G
ORDER BY loan_grade;
-- 
-- What we expect to see:
-- - Grade A: Low default rate (5-10%), low interest rate
-- - Grade G: High default rate (30-40%), high interest rate
-- - Grades should show clear progression

-- Check 4: Overall portfolio metrics
-- High-level statistics for the entire loan portfolio
SELECT 
    -- Total number of loans
    COUNT(*) as total_loans,
    -- COUNT(DISTINCT ...): Count unique values only
    -- How many unique customers? (some might have multiple loans)
    COUNT(DISTINCT customer_id) as unique_customers,
    -- Total dollar amount lent out
    ROUND(SUM(loan_amnt), 0) as total_loan_volume,
    -- Average loan size
    ROUND(AVG(loan_amnt), 0) as avg_loan_size,
    -- Total number of defaults
    SUM(loan_status) as total_defaults,
    -- Overall default rate across all loans
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as overall_default_rate,
    -- Average interest rate across all loans
    ROUND(AVG(loan_int_rate), 2) as avg_interest_rate
FROM loans;
-- 
-- Expected results:
-- total_loans: ~32,000
-- unique_customers: ~32,000 (assuming 1 loan per customer)
-- total_loan_volume: ~$400 million
-- avg_loan_size: ~$12,000
-- total_defaults: ~6,400
-- overall_default_rate: ~20%
-- avg_interest_rate: ~11%
