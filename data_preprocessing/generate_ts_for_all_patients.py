from extract_ts import extract_ts_from_subject
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

connection = psycopg2.connect(database="mimic", user=os.environ["USER_POSTGRES"])
cursor = connection.cursor()
cursor.execute("set search_path to mimiciii")

query_waveform_exists = """SELECT  *
    FROM sepsis3_cohort coh 
    WHERE coh.waveform_exists = 1
    ORDER BY subject_id"""
df_waveform_exists = pd.read_sql(query_waveform_exists, connection)

for index, row in df_waveform_exists.iterrows():
    extract_ts_from_subject(row)
