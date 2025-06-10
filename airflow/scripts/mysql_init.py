from datasets import load_dataset
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

DB_URI = (
    "mysql+pymysql://"
    + os.getenv("MYSQL_USER")
    + ":"
    + os.getenv("MYSQL_PASSWORD")
    + "@"
    + os.getenv("MYSQL_HOST")
    + "/"
    + os.getenv("MYSQL_DB")
)

engine = create_engine(DB_URI)

with engine.connect() as connection:
    connection.execute(text("CREATE DATABASE IF NOT EXISTS nlp_etl"))

    connection.execute(text("USE nlp_etl"))

    connection.execute(text("""
        CREATE TABLE IF NOT EXISTS input_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            text TEXT NOT NULL
        )
    """))

    connection.execute(text("""
        CREATE TABLE IF NOT EXISTS predicted_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            input_id INT,
            summary TEXT,
            FOREIGN KEY (input_id) REFERENCES input_data(id)
        )
    """))

dataset = load_dataset("cnn_dailymail", "3.0.0", split="validation[:50]")
df = pd.DataFrame(dataset["article"], columns=["text"])

df.to_sql("input_data", con=engine, if_exists="append", index=False, schema="nlp_etl")

print(f"Added {len(df)} records")
