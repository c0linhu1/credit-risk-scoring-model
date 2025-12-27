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

def train_evaluate_model(X, y):
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

    print(f"\nFeature Importance (which features have the most weight when making predictions)")
    perm_importance = permutation_importance(model, X_test_processed, y_test,
                                              n_repeats = 10, n_jobs = -1, random_state = 42)
    perm_importance_df = pd.DataFrame({
        'Feature': all_feature_names,
        'Importance': perm_importance.importances_mean
    }).sort_values('Importance', ascending = False)

    for feature, importance in zip(perm_importance_df['Feature'], perm_importance_df['Importance']):
        print(f'{feature}: {importance:.4f} ')

    return X_full_processed, y, model, y_test, y_pred_test, y_pred_test_probability, perm_importance_df

def cross_validation(X_full_processed, y, model):
    """Running 5-fold CV to verify model performance"""
    accuracy_score = cross_val_score(model, X_full_processed, y, cv = 5, scoring = 'accuracy')
    precision_score = cross_val_score(model, X_full_processed, y, cv = 5, scoring = 'precision')
    recall_score = cross_val_score(model, X_full_processed, y, cv = 5, scoring = 'recall')
    roc_auc_score = cross_val_score(model, X_full_processed, y, cv = 5, scoring = 'roc_auc')

    print(f"\nCV Accuracy: {accuracy_score.mean():.4f} (+/- {accuracy_score.std():.4f})")
    print(f"CV Precision: {precision_score.mean():.4f} (+/- {precision_score.std():.4f})")
    print(f"CV Recall: {recall_score.mean():.4f} (+/- {recall_score.std():.4f})")
    print(f"CV AUC-ROC: {roc_auc_score.mean():.4f} (+/- {roc_auc_score.std():.4f})")

def confusion_matrix_plot(y_test, y_pred_test):
    """Plotting confusion matrix heatmap"""
    cm = confusion_matrix(y_test, y_pred_test)
    plt.figure(figsize = (10, 6))
    sns.heatmap(cm, annot = True, fmt = 'd', cmap = 'Oranges', 
                xticklabels = ['Paid (0)', 'Default (1)'],
                yticklabels = ['Paid (0)', 'Default (1)'])

    plt.title('Confusion Matrix - XGBoost')
    plt.xlabel('Predicted')
    plt.ylabel('Actual')

    plt.tight_layout()
    # plt.savefig('Confusion_Matrix.png')
    plt.show()

def roc_curve_plot(y_test, y_pred_test_probability):
    """Plotting the ROC Curve"""
    # false positive rate, true positive rate
    fpr, tpr, thresholds = roc_curve(y_test, y_pred_test_probability)
    
    # area under the curve
    auc = roc_auc_score(y_test, y_pred_test_probability)

    plt.figure(figsize = (10, 6))

    plt.plot(fpr, tpr, color = 'orange', lw = 2, label = f'ROC CURVE (AUC = {auc:.4f})')
    plt.plot([0,1], [0,1], color = 'gray', linestyle = '--', label = 'Random Guess')

    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC Curve -  XGBoost')

    plt.legend(loc = 'lower right')
    plt.tight_layout()
    # plt.savefig('ROC_Curve.png')
    plt.show()

def feature_importance_plot(perm_importance_df):
    """Using a bar chart to plot the feature importance"""
    sorted = perm_importance_df.sort_values('Importance', ascending = True)

    plt.figure(figsize = (10, 6))
    plt.barh(sorted['Feature'], sorted['Importance'], color = 'orange')

    plt.xlabel('Importance')
    plt.ylabel('Feature')
    plt.yticks(fontsize = 7)
    plt.title('Feature Importance')
    plt.tight_layout()
    # plt.savefig('Feature_Importance.png')
    plt.show()
def main():
    data = get_data()
    print(data.head())

    X, y = data_prep(data)
    X_full_processed, y, model, y_test, y_pred_test, y_pred_test_probability, perm_importance_df = train_evaluate_model(X,y)
    cross_validation(X_full_processed, y, model)
    confusion_matrix_plot(y_test, y_pred_test)
    roc_curve_plot(y_test, y_pred_test_probability)
    feature_importance_plot(perm_importance_df)


if __name__ == "__main__":
    main()