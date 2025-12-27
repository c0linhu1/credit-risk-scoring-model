"""
xgboost.py
Using Extreme Gradient Boosting to predict loan defaults
Connects to PostgreSQL db instead of pulling from csv file
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score, confusion_matrix, classification_report, roc_curve
from sklearn.inspection import permutation_importance
from xgboost import XGBClassifier


load_dotenv()

def get_data():
    """Connecting to PostgreSQL and pulling loans data from table"""
    engine = create_engine(
        f"postgresql://{os.getenv('username')}:{os.getenv('password')}@{os.getenv('server_address_host')}:{os.getenv('port')}/{os.getenv('connection_name')}"
    )
    query = "SELECT * FROM loans"
    df = pd.read_sql(query, engine)
    engine.dispose()
    df = df.dropna()

    return df

def data_prep(data):
    features = [
        'person_age',
        'person_income',
        'person_home_ownership',
        'person_emp_length',
        'loan_intent',
        'loan_percent_income',
        'cb_person_default_on_file',
        'cb_person_cred_hist_length'
    ]
    X = data[features].copy()
    # trying to predict loan_status
    y = data['loan_status']

    return X, y

def train_evaluate_modek(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

    quantitative_features = ['person_age', 'person_income', 'person_emp_length',
                              'loan_percent_income', 'cb_person_cred_hist_length']
    
    qualitative_features = ['person_home_ownership', 'loan_intent', 'cb_person_default_on_file']

    preprocessor = ColumnTransformer(
        transformers = [
            ['qt_f', 'passthrough', quantitative_features],
            ['ql_f', OneHotEncoder(drop = 'first', sparse_output = False), qualitative_features]
        ]
    )

    X_train_processed = preprocessor.fit_transform(X_train)
    X_test_processed = preprocessor.transform(X_test)
    X_full_processed = preprocessor.transform(X)

    ql_f_names = preprocessor.named_transformers_['ql_f'].get_feature_names_out(qualitative_features)
    all_feature_names = quantitative_features + list(ql_f_names)

    model = XGBClassifier(
        n_estimators = 200,
        learning_rate = 0.1,
        subsample = 0.8,
        colsample_bytree = 0.8,
        scale_pos_weight = 3.5, 
        random_state = 42,
        n_jobs = -1,
    )

    model.fit(X_train_processed, y_train)

    y_pred_test = model.predict(X_test_processed)
    y_pred_test_probability = model.predict_proba(X_test_processed)[:, 1]

    print(f"\nAccuracy: {accuracy_score(y_test, y_pred_test):.4f}")
    print(f"Precision: {precision_score(y_test, y_pred_test):.4f}")
    print(f"Recall: {recall_score(y_test, y_pred_test):.4f}")
    print(f"AUC-ROC: {roc_auc_score(y_test, y_pred_test_probability):.4f}")

    print(f"\nConfusion Matrix:")
    print(confusion_matrix(y_test,y_pred_test))

    print(f"\nClassification Report:")
    print(classification_report(y_test, y_pred_test))

    


def main():
    data = get_data()
    print(data.head())


if __name__ == "__main__":
    main()