import pandas as pd
import logging
import os
from config import RAW_PATH, OUTPUT_PATH

logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_data():
    logging.info("Loading raw dataset")
    return pd.read_csv(RAW_PATH)

def clean_data(df):
    logging.info("Cleaning dataset")

    df = df.drop_duplicates()

    df.fillna({
        "CPU_Utilization (%)": df["CPU_Utilization (%)"].median(),
        "Memory_Usage (%)": df["Memory_Usage (%)"].median(),
        "Disk_IO (%)": df["Disk_IO (%)"].median(),
        "Network_Traffic_In (MB/s)": 0,
        "Network_Traffic_Out (MB/s)": 0,
        "Downtime (Hours)": 0
    }, inplace=True)

    df["Log_Timestamp"] = pd.to_datetime(df["Log_Timestamp"], errors="coerce")
    df = df.dropna(subset=["Log_Timestamp"])

    return df

def save_data(df):
    logging.info("Saving processed dataset")
    df.to_csv(OUTPUT_PATH, index=False)

def run_pipeline():
    try:
        df = load_data()
        df = clean_data(df)
        save_data(df)
        logging.info("Pipeline completed successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()