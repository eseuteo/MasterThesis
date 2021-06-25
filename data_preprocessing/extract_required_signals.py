import pandas as pd
import numpy as np

import psycopg2
import os
import wfdb
import urllib.request
import datetime

connection = psycopg2.connect(
    database="mimic", user="mimicuser", password=os.environ["PASSWORD"]
)
cursor = connection.cursor()
cursor.execute("set search_path to mimiciii")

query_waveform_exists = """SELECT  *
    FROM sepsis3_cohort coh 
    WHERE coh.waveform_exists = 1"""
df_waveform_exists = pd.read_sql(query_waveform_exists, connection)

signals_exist_array = np.empty(len(df_waveform_exists.index), dtype=str)
time_overlap_array = np.zeros(len(df_waveform_exists.index), dtype=bool)
number_of_overlapping_records = np.zeros(len(df_waveform_exists.index), dtype=np.int32)

for index, row in df_waveform_exists.iterrows():
    current_user_id = str(row.subject_id).zfill(6)
    wdb_base_path = "https://physionet.org/files/"
    wdb_dir_path = f"mimic3wdb-matched/1.0/p{current_user_id[:2]}/p{current_user_id}/"

    wdb_records = urllib.request.urlopen(wdb_base_path + wdb_dir_path + "RECORDS")

    count_overlapping_records = 0

    for line in wdb_records.readlines():
        record = line.decode("utf-8")
        record = str(record).rstrip()
        if record[-1] == "n":
            try:
                signals, fields = wfdb.rdsamp(record, pn_dir=wdb_dir_path)
                signals_names_list = [
                    item.upper().replace(" ", "") for item in fields["sig_name"]
                ]

                signals_exist = all(
                    x in signals_names_list
                    for x in ["HR", "ABPSYS", "ABPDIAS", "ABPMEAN", "RESP"]
                )
                signals_exist = signals_exist and any(
                    x in signals_names_list for x in ["%SPO2", "SPO2"]
                )

                if signals_exist:
                    signals_exist_array[index] = 1
                    record_start_time = datetime.datetime.combine(
                        fields["base_date"], fields["base_time"]
                    )

                    if "%.3f" % (fields["fs"]) == "1.000":
                        record_end_time = record_start_time + datetime.timedelta(
                            seconds=(fields["sig_len"] - 1)
                        )
                    elif "%.3f" % (fields["fs"]) == "0.017":
                        record_end_time = record_start_time + datetime.timedelta(
                            minutes=(fields["sig_len"] - 1)
                        )
                    else:
                        print("ERROR IN SAMPLING")
                    print(record)
                    print(wdb_dir_path)

                    # Caculate if we have a recording for the time of icu stay

                    time_range_icustay = (
                        datetime.datetime.strptime(
                            str(row["intime"]), "%Y-%m-%d %H:%M:%S"
                        ),
                        datetime.datetime.strptime(
                            str(row["outtime"]), "%Y-%m-%d %H:%M:%S"
                        ),
                    )
                    time_range_record = (record_start_time, record_end_time)

                    latest_start = max(time_range_icustay[0], time_range_record[0])
                    earliest_end = min(time_range_icustay[0], time_range_record[0])
                    delta_earliest_end_latest_start = (
                        earliest_end - latest_start
                    ).days + 1

                    if delta_earliest_end_latest_start >= 0:
                        time_overlap_array[index] = 1
                        count_overlapping_records += 1
                        # todo : adding new dataframe, extracting required signals, computing avergage for per sminute values in case of per second sampling frequency
            except ValueError:
                print(f"Error occurred while reading waveform: {record}")
                df_waveform_exists["signal_exists"] = signal_exists_array
                df_waveform_exists["time_overlap"] = time_overlap_array
                df_waveform_exists[
                    "number_of_overlapping_records"
                ] = number_of_overlapping_records
                df_waveform_exists.to_csv("required_signals_df.csv")
    number_of_overlapping_records[index] = count_overlapping_records

df_waveform_exists["signal_exists"] = signal_exists_array
df_waveform_exists["time_overlap"] = time_overlap_array
df_waveform_exists["number_of_overlapping_records"] = number_of_overlapping_records

df_waveform_exists.to_csv("required_signals_df.csv")
