import os
import datetime
import pandas as pd

def log_ingestion():
    raw_dir = r"C:\Users\jared\Documents\Universidad\8vo semestre\Big Data\Amazon Big Data\data\raw"
    log_file = "ingestion_log.txt"
    
    with open(log_file, "a") as f:
        for file in os.listdir(raw_dir):
            if file.endswith(".csv"):
                filepath = os.path.join(raw_dir, file)
                # Usamos pandas solo para contar rápido en la ingesta
                df = pd.read_csv(filepath)
                row_count = len(df)
                
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_entry = f"[{timestamp}] SUCCESS: Ingested '{file}' | Destination: RAW_LAYER | Rows: {row_count}\n"
                f.write(log_entry)
                print(log_entry)

if __name__ == "__main__":
    log_ingestion()