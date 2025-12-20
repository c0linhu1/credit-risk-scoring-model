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
from sklearn.preprocessing import StandardScaler, LabelEncoder

load_dotenv()

def get_data():
    """Connecting to PostgreSQL and pulling loans data from table"""
    engine = create_engine(
        f"postgresql://{os.getenv('username')}:{os.getenv('password')}@{os.getenv('server_address_host')}:{os.getenv('port')}/{os.getenv('connection_name')}"
    )
    query = "SELECT * FROM loans"
    df = pd.read_sql(query, engine)
    engine.dispose()

    return df


def main():
    data = get_data()
    print(data.head())

if __name__ == "__main__":
    main()