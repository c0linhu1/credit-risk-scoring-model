# Credit Risk Scoring Model

Analyzing loan default patterns to identify key risk factors for lending decisions using SQL, Python, and Tableau.

## Project Overview

**Business Question:** What borrower/loan characteristics most strongly predict in somebody defaulting, and how can lenders use this information to factor the amount of risk that they want to take?

**Key Finding:** Debt-to-income ratio (DTI) is the strongest predictor of default, followed by income level and prior default history. Borrowers with multiple risk factors have dramatically higher default rates (7% with 0 flags → 100% with 6 flags).

## Data Source
- Original source: Kaggle Credit Risk Dataset
- Contains 12 columns of 30,000+ rows on data
- Head Format:
    person_age,person_income,person_home_ownership,person_emp_length,loan_intent,loan_grade,loan_amnt,loan_int_rate,loan_status,loan_percent_income,cb_person_default_on_file,cb_person_cred_hist_length
    22,59000,RENT,123.0,PERSONAL,D,35000,16.02,1,0.59,Y,3
    21,9600,OWN,5.0,EDUCATION,B,1000,11.14,0,0.1,N,2
    25,9600,MORTGAGE,1.0,MEDICAL,C,5500,12.87,1,0.57,N,3
    23,65500,RENT,4.0,MEDICAL,C,35000,15.23,1,0.53,N,2
    24,54400,RENT,8.0,MEDICAL,C,35000,14.27,1,0.55,Y,4

    'loan_intent': {['DEBTCONSOLIDATION', 'EDUCATION', 'HOMEIMPROVEMENT', 'MEDICAL', 'PERSONAL', 'VENTURE']}
    'loan_grade': {['A', 'B', 'C', 'D', 'E', 'F', 'G']} (low risk to high risk)

## Tech Stack
- Database: PostgreSQL (AWS RDS)
- Languages: SQL, Python
- ML Libraries: scikit-learn, XGBoost
- Visualization: Matplotlib, Seaborn, Tableau

## SQL Analysis - Views Created

**Basic Analysis (analysis.sql):**
- overall_stats — Portfolio-level metrics
- default_rate_by_age — Default rate by age bracket
- default_rate_by_income — Default rate by income bracket
- default_rate_by_home_ownership — Default rate by housing status
- default_rate_by_emp_length — Default rate by employment length
- default_rate_by_loan_intent — Default rate by loan purpose
- default_rate_by_dti — Default rate by DTI bracket
- default_rate_by_past_defaults — Default rate by prior default history
- default_rate_by_credit_history_length — Default rate by credit history

**Advanced Analysis (analysis_2.sql):**
- risk_deciles — Borrowers split into 10 risk groups using NTILE() window function
- cumulative_defaults_by_dti — Cumulative default distribution using CTEs and SUM() OVER()
- default_rate_by_income_percentile — 20 income percentiles using NTILE(20)
- credit_scoring_model — Multi-factor risk scoring combining 6 risk flags - see analysis_2.sql

## Machine Learning Models
**Models Compared:**
- XGBoost 
- Random Forest
- Logistic Regression

Best Model: XGBoost - highest Recall and AUC-ROC for catching defaults

## Loading Data
**SQL Files:**
psql credit_risk_db \i sql_files/setup.sql
- Details to database in .env

## Business Insights
1. Flag high-DTI applicants — Borrowers with DTI >30% should receive additional review
2. Weight prior defaults heavily — Past default practically doubles default probability
3. Use multi-factor scoring — Stacking risk flags is highly predictive
4. Income thresholds — Consider minimum income requirements for larger loans
5. Loan purpose matters — Be wary of debt consolidation and medical loans as they carry higher risk

## Future Improvements
- Use SMOTE for class imbalance

Colin Hui