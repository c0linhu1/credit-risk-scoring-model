/*
analysis.sql
Analyzing different characteristics of loans dataset - see which have higher default rates

Using 
 - person_age
 - person_income
 - person_home_ownership
 - person_emp_length
 - loan_intent
 - loan_percent_income (rather than doing loan amount because this normalizes for income)
 - cb_person_default_on_file
 - cb_person_cred_hist_length
*/


-- Finding overall stats - creating overall (average) stats 
--CREATE VIEW overall_stats AS 
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
FROM loans