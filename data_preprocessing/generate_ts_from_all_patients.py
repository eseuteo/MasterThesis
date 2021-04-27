from extract_ts import extract_ts_from_subject
import pandas as pd

df = pd.read_csv("generated_files/missing_values.csv")

for index, row in df.iterrows():
    subject_id = row["subject_id"]
