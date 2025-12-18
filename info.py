import pandas as pd
import numpy as np

FILE = 'credit_risk_dataset.csv'

def load_data(FILE):
    return pd.read_csv(FILE)


def loan_intent_reasons(data):
    reasons = data['loan_intent'].unique()
    print(sorted(reasons))

def loan_grade_distribution(data):
    grade_distribution = data['loan_grade'].unique()
    print(sorted(grade_distribution))

def home_ownership(data):
    home_type = data['person_home_ownership'].unique()
    print(sorted(home_type))

def main():
    
    data = load_data(FILE)
    loan_intent_reasons(data)
    loan_grade_distribution(data)
    home_ownership(data)





if __name__ == '__main__':
    main()