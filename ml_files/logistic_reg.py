"""
logistic_reg.py
Using logistic regression to predicting loan defaults 
Connects to PostgreSQL db instead of pulling from csv file
"""

import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score, confusion_matrix, classification_report
from sklearn.model_selection import cross_val_score
import matplotlib.pyplot as plt
import seaborn as sns


load_dotenv()

def get_data():
    """Connecting to PostgreSQL and pulling loans data from table"""
    engine = create_engine(
        f"postgresql://{os.getenv('username')}:{os.getenv('password')}@{os.getenv('server_address_host')}:{os.getenv('port')}/{os.getenv('connection_name')}"
    )
    query = "SELECT * FROM loans"
    df = pd.read_sql(query, engine)
    engine.dispose()
    #print(df.shape)
    df = df.dropna()
    #print(df.shape)
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

def train_evaluate_model(X, y):
    """Splitting data and scaling some featues - using onehotencoder bc it has no implied order like ordinal and label encoder"""
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

    quantitative_features = ['person_age', 'person_income', 'person_emp_length', 'loan_percent_income', 'cb_person_cred_hist_length' ]
    qualitative_features = ['person_home_ownership', 'loan_intent', 'cb_person_default_on_file']

    preprocessor = ColumnTransformer(
        transformers = [
            ('qt_f', StandardScaler(), quantitative_features),
            ('ql_f', OneHotEncoder(drop = 'first', sparse_output = False), qualitative_features)
        ]
    )

    # we also have to fit preprocessor on the train/test data
    X_train_processed = preprocessor.fit_transform(X_train)
    X_test_processed = preprocessor.transform(X_test)

    # processing entire dataset for future CV
    X_full_processed = preprocessor.transform(X)

    ql_f_names = preprocessor.named_transformers_['ql_f'].get_feature_names_out(qualitative_features)
    # print(ql_f_names)

    all_feature_names = quantitative_features + list(ql_f_names)
    # print(all_feature_names)

    # default max_iter is 100 raising to 1000 due to greater amount of features
    model = LogisticRegression(max_iter = 1000, random_state = 42)
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

    print(f"\nFeature Coefficients (Impact on Default Probability):")
    coef_df = pd.DataFrame({
        'Feature': all_feature_names,
        'Coefficient': model.coef_[0]
    })

    for feature, coefficient in zip(coef_df['Feature'], coef_df['Coefficient']):
        if coefficient > 0:
            print(f"{feature} {coefficient:.4f} increases default risk")
        else:
            print(f"{feature} {coefficient:.4f} decreases default risk")

    return X_full_processed, y, model, y_test, y_pred_test, y_pred_test_probability, coef_df

def cross_validation(X_processed, y, model):
    """Running 5 fold CV to verify model performance"""
    accuracy_score = cross_val_score(model, X_processed, y, cv = 5, scoring = 'accuracy')
    precision_score = cross_val_score(model, X_processed, y, cv = 5, scoring = 'precision')
    recall_score = cross_val_score(model, X_processed, y, cv = 5, scoring = 'recall')
    roc_auc_score = cross_val_score(model,X_processed, y, cv = 5, scoring = 'roc_auc')

    print(f"\nCV Accuracy: {accuracy_score.mean():.4f} (+/- {accuracy_score.std():.4f})")
    print(f"CV Precision: {precision_score.mean():.4f} (+/- {precision_score.std():.4f})")
    print(f"CV Recall: {recall_score.mean():.4f} (+/- {recall_score.std():.4f})")
    print(f"CV AUC-ROC: {roc_auc_score.mean():.4f} (+/- {roc_auc_score.std():.4f})")

def confusion_matrix_plot(y_test, y_pred):
    """Plotting confusion matrix heatmap"""
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize = (10,6))

    sns.heatmap(cm, annot = True, fmt = 'd', cmap = 'Greens', 
                xticklabels = ['Paid (0)', 'Defaulted (1)'],
                yticklabels = ['Paid (0)', 'Defaulted (1)'])
    
    plt.title('Confusion Matrix - Logistic Regression')
    plt.xlabel('Predicted')
    plt.ylabel('Actual')

    plt.tight_layout()
    #plt.savefig('Logistic_Regression_Confusion_Matrix.png')
    plt.show()
    print('Viz saved')
                

def main():
    data = get_data()
    print(data.head())

    X, y = data_prep(data)
    X_full_processed, y, model, y_test, y_pred_test, y_pred_test_probability, coef_df = train_evaluate_model(X, y)

    cross_validation(X_full_processed, y, model)

    confusion_matrix_plot(y_test, y_pred_test)
if __name__ == "__main__":
    main()