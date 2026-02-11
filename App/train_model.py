import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
import joblib
import json  # Added to save the score
import os

# Ensure model directory exists
if not os.path.exists('model'):
    os.makedirs('model')


def train():
    print("1. Loading data...")
    try:
        df = pd.read_csv("data/immo_data.csv", encoding='latin1')
    except FileNotFoundError:
        print("Error: 'data/immo_data.csv' not found.")
        return

    # --- Preprocessing ---
    print("2. Preprocessing data...")

    if 'regio1' in df.columns:
        df = df.rename(columns={'regio1': 'state'})
    elif 'state' not in df.columns:
        print("Error: Dataset missing 'regio1' or 'state' column.")
        return

    df = df.drop_duplicates()
    df = df[df["livingSpace"] != 0]

    df['totalRent'] = pd.to_numeric(df['totalRent'], errors='coerce')
    df['baseRent'] = pd.to_numeric(df['baseRent'], errors='coerce')

    df = df[df['totalRent'] > df['baseRent']]
    df = df.dropna(subset=['totalRent', 'livingSpace', 'noRooms', 'yearConstructed'])

    # Fix Boolean columns for Scikit-Learn
    bool_cols = ['balcony', 'newlyConst']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)

    feature_cols = ['livingSpace', 'noRooms', 'heatingType', 'balcony', 'newlyConst', 'yearConstructed', 'state']
    target_col = 'totalRent'

    X = df[feature_cols]
    y = df[target_col]

    # --- Pipeline ---
    print("3. Building Model Pipeline...")

    numeric_features = ['livingSpace', 'noRooms', 'yearConstructed']
    numeric_transformer = SimpleImputer(strategy='median')

    categorical_features = ['heatingType', 'state']
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
        ('onehot', OneHotEncoder(handle_unknown='ignore'))
    ])

    boolean_features = ['balcony', 'newlyConst']
    boolean_transformer = SimpleImputer(strategy='most_frequent')

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features),
            ('bool', boolean_transformer, boolean_features)
        ])

    model = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1))
    ])

    # --- Train and Save ---
    print("4. Training Model...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model.fit(X_train, y_train)
    score = model.score(X_test, y_test)
    print(f"   Model R^2 Score: {score:.2f}")

    # SAVE MODEL
    joblib.dump(model, 'model/housing_model.pkl')

    # SAVE ACCURACY SCORE (New Feature)
    with open('model/metrics.json', 'w') as f:
        json.dump({"r2_score": score}, f)

    print("5. Success! Model and metrics saved.")


if __name__ == "__main__":
    train()
    