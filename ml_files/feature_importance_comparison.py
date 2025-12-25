"""
importance_comparison.py
Comparing Gini vs Permutation importance methods by lookinh at sklearn metrics
"""

import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score
from sklearn.inspection import permutation_importance

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
    y = data['loan_status']

    return X, y

def compare_importance_methods(X, y):
    """training model - getting both impotance type and comparing metrics"""
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

    quantitative_features = ['person_age', 'person_income', 'person_emp_length',
                              'loan_percent_income', 'cb_person_cred_hist_length']
    
    qualitative_features = ['person_home_ownership', 'loan_intent', 'cb_person_default_on_file']

    preprocessor = ColumnTransformer(
        transformers = [
            ('qt_f', 'passthrough', quantitative_features),
            ('ql_f', OneHotEncoder(drop = 'first', sparse_output = False), qualitative_features)
        ]
    )

    X_train_processed = preprocessor.fit_transform(X_train)
    X_test_processed = preprocessor.transform(X_test)

    # this returns the qualitative feature names as a numpy array - need to convert to list 
    ql_f_names = preprocessor.named_transformers_['ql_f'].get_feature_names_out(qualitative_features)
    # print(ql_f_names)
    all_feature_names = quantitative_features + list(ql_f_names)

    model = RandomForestClassifier(n_estimators = 200, random_state = 42, n_jobs = -1)
    model.fit(X_train_processed, y_train)
    
    gini_importance_df = pd.DataFrame({
        'Feature': all_feature_names,
        'Importance': model.feature_importances_
    }).sort_values('Importance', ascending = False)

    perm_importance = permutation_importance(model, X_test_processed, y_test, 
                                             n_repeats = 10, random_state = 42, n_jobs = -1)
    perm_importance_df = pd.DataFrame({
        'Feature': all_feature_names,
        'Importance': perm_importance.importances_mean
    }).sort_values('Importance', ascending = False)

    for i in range(len(all_feature_names)):
        gini_feat = gini_importance_df.iloc[i]['Feature']
        gini_feat_value = gini_importance_df.iloc[i]['Importance']
        perm_feat = perm_importance_df.iloc[i]['Feature']
        perm_feat_value = perm_importance_df.iloc[i]['Importance']
        print(f"{gini_feat}: {gini_feat_value:.4f} \t\t\t {perm_feat}: {perm_feat_value:.4f}")

    for n_features in [3, 5, 7]:
        print(f"\nMetrics For The Top {n_features} Features")
        
        # getting top features for both methods
        top_gini = gini_importance_df.head(n_features)['Feature'].tolist()
        top_perm = perm_importance_df.head(n_features)['Feature'].tolist()
        
        # getiing specific column indices
        gini_indices = [all_feature_names.index(f) for f in top_gini]
        perm_indices = [all_feature_names.index(f) for f in top_perm]
        
        # training with gini features
        model_gini = RandomForestClassifier(n_estimators = 200, random_state = 42, n_jobs = -1)
        model_gini.fit(X_train_processed[:, gini_indices], y_train)
        gini_pred = model_gini.predict(X_test_processed[:, gini_indices])
        gini_proba = model_gini.predict_proba(X_test_processed[:, gini_indices])[:, 1]
        
        # training w perm features
        model_perm = RandomForestClassifier(n_estimators = 200, random_state = 42, n_jobs = -1)
        model_perm.fit(X_train_processed[:, perm_indices], y_train)
        perm_pred = model_perm.predict(X_test_processed[:, perm_indices])
        perm_proba = model_perm.predict_proba(X_test_processed[:, perm_indices])[:, 1]
        
        # getting all relevant metrics
        gini_acc = accuracy_score(y_test, gini_pred)
        gini_prec = precision_score(y_test, gini_pred)
        gini_rec = recall_score(y_test, gini_pred)
        gini_auc = roc_auc_score(y_test, gini_proba)
        
        perm_acc = accuracy_score(y_test, perm_pred)
        perm_prec = precision_score(y_test, perm_pred)
        perm_rec = recall_score(y_test, perm_pred)
        perm_auc = roc_auc_score(y_test, perm_proba)
        
        print(f"\n{'Metric':<12}{'Gini':<12}{'Permutation':<12}")
        print("-" * 50)
        
        # Accuracy
        print(f"{'Accuracy':<12}{gini_acc:<12.4f}{perm_acc:<12.4f}")
        
        # Precision
        print(f"{'Precision':<12}{gini_prec:<12.4f}{perm_prec:<12.4f}")
        
        # Recall
        print(f"{'Recall':<12}{gini_rec:<12.4f}{perm_rec:<12.4f}")
        
        # AUC-ROC
        print(f"{'AUC-ROC':<12}{gini_auc:<12.4f}{perm_auc:<12.4f}")

    # full model using all features
    print("\nFull Model Using All Features")
    print(f"{'-'* 50}")
    full_pred = model.predict(X_test_processed)
    full_proba = model.predict_proba(X_test_processed)[:, 1]
    print(f"Accuracy: {accuracy_score(y_test, full_pred):.4f}")
    print(f"Precision: {precision_score(y_test, full_pred):.4f}")
    print(f"Recall: {recall_score(y_test, full_pred):.4f}")
    print(f"AUC-ROC: {roc_auc_score(y_test, full_proba):.4f}")

def main():
    data = get_data()
    X, y = data_prep(data)
    
    compare_importance_methods(X, y)

if __name__ == '__main__':
    main()