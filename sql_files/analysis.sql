/*
analysis.sql
Analyzing different characteristics of loans dataset - see which have higher default rates

Using features
 - person_age
 - person_income
 - person_home_ownership
 - person_emp_length
 - loan_intent
 - loan_percent_income (rather than doing loan amount because this normalizes for income)
 - cb_person_default_on_file
 - cb_person_cred_hist_length
*/

DROP VIEW IF EXISTS overall_stats, default_rate_by_age, default_rate_by_income, default_rate_by_home_ownership,
default_rate_by_emp_length, default_rate_by_loan_intent, default_rate_by_dti, default_rate_by_past_defaults, 
default_rate_by_credit_history_length;


-- Finding overall stats - creating overall (average) stats 
CREATE VIEW overall_stats AS 
SELECT
    ROUND(AVG(person_age), 2) as avg_age,
    ROUND(AVG(person_income), 2) as avg_income,
    (SELECT person_home_ownership
     FROM loans
     GROUP BY person_home_ownership
     ORDER BY COUNT(*) DESC
     LIMIT 1
    ) as most_common_home_ownership,
    ROUND(AVG(person_emp_length), 2) as avg_employed_length,
    (SELECT loan_intent
     FROM loans
     GROUP BY loan_intent
     ORDER BY COUNT(*) DESC
     LIMIT 1
    ) as most_common_loan_intent,
    ROUND(AVG(loan_percent_income), 4) as avg_dti, -- debt to income ratio
    ROUND(100.0 * SUM(
        CASE 
            WHEN cb_person_default_on_file = 'Y' THEN 1 
            ELSE 0
        END
    ) / COUNT(*), 2
    ) as pct_past_defaults,
    ROUND(AVG(cb_person_cred_hist_length), 2) as avg_credit_history,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as overall_default_rate
FROM loans;

--SELECT * FROM overall_stats;

CREATE VIEW default_rate_by_age AS
SELECT
    CASE
        WHEN person_age BETWEEN 18 AND 24 THEN '18-24'
        WHEN person_age BETWEEN 25 AND 32 THEN '25-32'
        WHEN person_age BETWEEN 33 AND 45 THEN '33-45'
        WHEN person_age BETWEEN 46 AND 55 THEN '46-55'
        WHEN person_age > 55 THEN '55+'
    END AS age_range,
    COUNT(*) AS total_loans,
    SUM(loan_status) AS defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate
FROM loans
GROUP BY age_range
ORDER BY age_range;

--SELECT * FROM default_rate_by_age;

CREATE VIEW default_rate_by_income AS 
SELECT
    CASE 
        WHEN person_income < 25000 THEN 1
        WHEN person_income BETWEEN 25000 AND 49999 THEN 2
        WHEN person_income BETWEEN 50000 AND 74999 THEN 3
        WHEN person_income BETWEEN 75000 and 99999 THEN 4
        WHEN person_income BETWEEN 100000 AND 149999 THEN 5
        WHEN person_income >= 150000 THEN 6
    END AS row_num,
    CASE 
        WHEN person_income < 25000 THEN '0-25k'
        WHEN person_income BETWEEN 25000 AND 49999 THEN '25k-50k'
        WHEN person_income BETWEEN 50000 AND 74999 THEN '50k-75k'
        WHEN person_income BETWEEN 75000 and 99999 THEN '75k-100k'
        WHEN person_income BETWEEN 100000 AND 149999 THEN '100k-150k'
        WHEN person_income >= 150000 THEN '150k+'
    END AS income_range,
    COUNT(*) AS total_loans,
    SUM(loan_status) AS defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate
FROM loans
GROUP BY row_num, income_range
ORDER BY row_num ASC;

--SELECT * FROM default_rate_by_income;

CREATE VIEW default_rate_by_home_ownership AS
SELECT
    person_home_ownership AS home_type,
    COUNT(*) AS total_loans,
    SUM(loan_status) AS defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate
FROM loans
GROUP BY home_type
ORDER BY home_type;

--SELECT * FROM default_rate_by_home_ownership;

CREATE VIEW default_rate_by_emp_length AS 
SELECT
    CASE
        WHEN person_emp_length BETWEEN 0 AND 2 THEN 1
        WHEN person_emp_length BETWEEN 3 AND 5 THEN 2
        WHEN person_emp_length BETWEEN 6 AND 10 THEN 3
        WHEN person_emp_length > 10 THEN 4
    END AS row_num,
    CASE
        WHEN person_emp_length BETWEEN 0 AND 2 THEN '0-2 yrs'
        WHEN person_emp_length BETWEEN 3 AND 5 THEN '3-5 yrs'
        WHEN person_emp_length BETWEEN 6 AND 10 THEN '6-10 yrs'
        WHEN person_emp_length > 10 THEN '10+ yrs'
    END AS employment_length,
    -- 'person_emp_length' only column w nulls
    COUNT(person_emp_length) AS total_loans,
    SUM(loan_status) AS defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(person_emp_length), 2) as default_rate
FROM loans
WHERE person_emp_length IS NOT NULL
GROUP BY row_num, employment_length
ORDER BY row_num;

--SELECT * FROM default_rate_by_emp_length;

CREATE VIEW default_rate_by_loan_intent AS 
SELECT 
    loan_intent,
    COUNT(*) AS total_loans,
    SUM(loan_status) AS defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate
FROM loans
GROUP BY loan_intent
ORDER BY loan_intent;

--SELECT * FROM default_rate_by_loan_intent;

CREATE VIEW default_rate_by_dti AS
SELECT
    CASE
        WHEN loan_percent_income BETWEEN 0 AND 0.1 THEN '0-10%'
        WHEN loan_percent_income BETWEEN 0.1001 AND 0.2 THEN '10-20%'
        WHEN loan_percent_income BETWEEN 0.2001 AND 0.3 THEN '20-30%'
        WHEN loan_percent_income BETWEEN 0.3001 AND 0.4 THEN '30-40%'
        WHEN loan_percent_income BETWEEN 0.4001 AND 0.5 THEN '40-50%'
        WHEN loan_percent_income > 0.5 THEN '50%+'
    END AS dti_bracket,
    COUNT(*) AS total_loans,
    SUM(loan_status) AS defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) AS default_rate
FROM loans
GROUP BY dti_bracket
ORDER BY dti_bracket;

--SELECT * FROM default_rate_by_dti;

CREATE VIEW default_rate_by_past_defaults AS
SELECT  
    cb_person_default_on_file AS past_default,
    COUNT(*) as total_loans,
    SUM(loan_status) as defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate
FROM loans
GROUP BY past_default;

--SELECT * FROM default_rate_by_past_defaults;

CREATE VIEW default_rate_by_credit_history_length AS 
SELECT
    CASE 
        WHEN cb_person_cred_hist_length BETWEEN 0 AND 2 THEN 1
        WHEN cb_person_cred_hist_length BETWEEN 3 AND 5 THEN 2
        WHEN cb_person_cred_hist_length BETWEEN 6 AND 10 THEN 3
        WHEN cb_person_cred_hist_length BETWEEN 11 AND 20 THEN 4
        WHEN cb_person_cred_hist_length > 20 THEN 5
    END AS row_num,
    CASE 
        WHEN cb_person_cred_hist_length BETWEEN 0 AND 2 THEN '0-2 yrs'
        WHEN cb_person_cred_hist_length BETWEEN 3 AND 5 THEN '3-5 yrs'
        WHEN cb_person_cred_hist_length BETWEEN 6 AND 10 THEN '6-10 yrs'
        WHEN cb_person_cred_hist_length BETWEEN 11 AND 20 THEN '11-20 yrs'
        WHEN cb_person_cred_hist_length > 20 THEN '20+ yrs'
    END AS credit_history_range,
    COUNT(*) as total_loans,
    SUM(loan_status) as defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*)) as default_rate
FROM loans
GROUP BY row_num, credit_history_range
ORDER BY row_num;

--SELECT * FROM default_rate_by_credit_history_length;