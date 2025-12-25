import pandas as pd 
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score
from sklearn.inspection import permutation_importance

load_dotenv()

def get_data():
    engine = create_engine(
        
    )