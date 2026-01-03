/*
analysis_2.sql
Creating more views using slightly more advanced sql  
*/

DROP VIEW IF EXISTS risk_deciles, cumulative_defaults_by_dti, default_rate_by_income_percentile, credit_scoring_model;

-- show how default rate increases across all deciles
CREATE VIEW risk_deciles AS
WITH deciles AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY loan_percent_income) as risk_group
    FROM loans
)
SELECT
    risk_group,
    COUNT(*) as total_loans,
    SUM(loan_status) as defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) as default_rate,
    ROUND(MIN(loan_percent_income), 2) as min_dti,
    ROUND(MAX(loan_percent_income), 2) as max_dti,
    ROUND(AVG(person_income), 0) as avg_income
FROM deciles
GROUP BY risk_group
ORDER BY risk_group;

-- SELECT * FROM risk_deciles;

-- shows how defaults accumuate as DTI increases 
CREATE VIEW cumulative_defaults_by_dti AS 
WITH dti_stats AS (
    SELECT
        CASE 
            WHEN loan_percent_income <= 0.1 THEN 1
            WHEN loan_percent_income <= 0.2 THEN 2
            WHEN loan_percent_income <= 0.3 THEN 3
            WHEN loan_percent_income <= 0.4 THEN 4
            WHEN loan_percent_income <= 0.5 THEN 5
            ELSE 6
        END AS dti_order,
        CASE 
            WHEN loan_percent_income <= 0.1 THEN '0-10%'
            WHEN loan_percent_income <= 0.2 THEN '10-20%'
            WHEN loan_percent_income <= 0.3 THEN '20-30%'
            WHEN loan_percent_income <= 0.4 THEN '30-40%'
            WHEN loan_percent_income <= 0.5 THEN '40-50%'
            ELSE '50%+'
        END AS dti_bracket,
        COUNT(*) as total_loans,
        SUM(loan_status) as defaults
    FROM loans
    GROUP BY dti_order, dti_bracket
), 
with_totals AS (
    SELECT 
        dti_order,
        dti_bracket,
        total_loans,
        defaults,
        ROUND(100.0 * defaults / total_loans, 2) as default_rate,
        SUM(defaults) OVER (ORDER BY dti_order) as cumulative_defaults,
        SUM(total_loans) OVER (ORDER BY dti_order) as cumulative_loans,
        SUM(defaults) OVER () as total_all_defaults
    FROM dti_stats
)
SELECT
    dti_order,
    dti_bracket,
    total_loans,
    defaults,
    default_rate,
    cumulative_defaults,
    cumulative_loans,
    total_all_defaults,
    ROUND(100.0 * cumulative_defaults / total_all_defaults, 2) as pct_all_defaults
FROM with_totals
ORDER BY dti_order;

-- SELECT * FROM cumulative_defaults_by_dti;

-- shows default_rate by income bracket
CREATE VIEW default_rate_by_income_percentile AS 
WITH income_ranked AS (
    SELECT
        *,
        NTILE(20) OVER (ORDER BY person_income) as income_group
    FROM loans
)
SELECT 
    income_group,
    COUNT(*) AS total_loans,
    SUM(loan_status) AS defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) AS default_rate,
    MIN(person_income) AS min_income,
    MAX(person_income) AS max_income,
    ROUND(AVG(loan_percent_income), 2) AS avg_dti
FROM income_ranked
GROUP BY income_group
ORDER BY income_group;

-- SELECT * FROM default_rate_by_income_percentile;

-- creating a credit scoring system
CREATE VIEW credit_scoring_model AS
WITH risk_factors AS (
    SELECT
        *,
        CASE WHEN loan_percent_income > 0.3 THEN 1 ELSE 0 END AS high_dti,
        CASE WHEN person_income < 25000 THEN 1 ELSE 0 END AS low_income,
        CASE WHEN person_home_ownership = 'RENT' THEN 1 ELSE 0 END AS is_rentor,
        CASE WHEN loan_intent = 'DEBTCONSOLIDATION' THEN 1 ELSE 0 END AS is_debt_consolidation,
        CASE WHEN person_emp_length < 2 THEN 1 ELSE 0 END AS short_employment,
        CASE WHEN cb_person_default_on_file = 'Y' THEN 1 ELSE 0 END AS past_default
    FROM loans
    WHERE person_emp_length IS NOT NULL
),
risk_calculator AS (
    SELECT 
        *,
        high_dti + low_income + is_rentor + is_debt_consolidation + short_employment + past_default AS risk_flag_count
    FROM risk_factors
)
SELECT 
    risk_flag_count,
    COUNT(*) as total_loans,
    SUM(loan_status) AS defaults,
    ROUND(100.0 * SUM(loan_status) / COUNT(*), 2) AS default_rate,
    ROUND(AVG(person_income), 2) AS avg_income,
    ROUND(AVG(loan_percent_income), 2) as avg_dti 
FROM risk_calculator
GROUP BY risk_flag_count
ORDER BY risk_flag_count;

-- SELECT * FROM credit_scoring_model;

