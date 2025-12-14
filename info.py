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

def main():
    
    data = load_data(FILE)
    loan_intent_reasons(data)
    loan_grade_distribution(data)





if __name__ == '__main__':
    main()