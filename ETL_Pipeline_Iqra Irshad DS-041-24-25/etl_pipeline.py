# Name: Iqra Irshad
# Roll Number: DS-041/24-25
# etl_pipeline.py
import pandas as pd
import json
import datetime
from utils import clean_data, convert_temperature, normalize_timestamp, engineer_features

# Load CSV
def extract_csv(path):
    return pd.read_csv(path)

# Load JSON
def extract_json(path):
    with open(path) as f:
        data = json.load(f)
    return pd.json_normalize(data)

# Load Google Sheet CSV
extract_google_sheet = extract_csv

def transform(df):
    df = clean_data(df)
    df = convert_temperature(df)
    df = normalize_timestamp(df)
    df = engineer_features(df)
    return df

def run_pipeline():
    print("Extracting data...")
    csv_df = extract_csv("data/sample_data.csv")
    json_df = extract_json("data/sample_weather.json")
    google_df = extract_google_sheet("data/google_sheet_sample.csv")

    print("Combining data...")
    df = pd.concat([csv_df, google_df], ignore_index=True)

    print("Transforming data...")
    df = transform(df)

    print("Saving cleaned data...")
    df.to_csv("output/final_cleaned_data.csv", index=False)

if __name__ == '__main__':
    run_pipeline()