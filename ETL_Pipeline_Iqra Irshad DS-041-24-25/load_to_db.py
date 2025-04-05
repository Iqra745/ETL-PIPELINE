import pandas as pd
from sqlalchemy import create_engine
import json

def load_to_db(csv_path, config_path="config/db_config.json"):
    with open(config_path) as f:
        config = json.load(f)

    db_url = f"{config['dialect']}://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    engine = create_engine(db_url)

    df = pd.read_csv(csv_path)
    df.to_sql('cleaned_data', engine, if_exists='replace', index=False)
    print("Data loaded to database successfully.")

if __name__ == '__main__':
    load_to_db("output/final_cleaned_data.csv")