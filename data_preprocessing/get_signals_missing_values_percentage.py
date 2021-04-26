import pandas as pd
import numpy as np

import psycopg2
import os
import wfdb
import urllib.request
import datetime
import re

from utils import (
    get_record_from_line,
    get_time_range_icustay,
    get_time_range_record,
    get_signal_index,
    ranges_overlap,
    get_delta_earliest_end_latest_start,
)


def export_csv(
    subject_id_column,
    number_of_icu_stays_column,
    percentage_missing_values_column,
    required_signals_available_column,
    filename,
):
    df_missing_values = pd.DataFrame(
        columns=df_missing_values_columns,
    )

    df_missing_values["subject_id"] = subject_id_column
    df_missing_values["number_of_icu_stays"] = number_of_icu_stays_column
    df_missing_values["percentage_missing_values"] = percentage_missing_values_column
    df_missing_values["required_signals_available"] = required_signals_available_column

    df_missing_values.to_csv(filename)


connection = psycopg2.connect(database="mimic", user="holthausen")
cursor = connection.cursor()
cursor.execute("set search_path to mimiciii")

query_waveform_exists = """SELECT  *
    FROM sepsis3_cohort coh 
    WHERE coh.waveform_exists = 1"""
df_waveform_exists = pd.read_sql(query_waveform_exists, connection)

required_signals_list = ["HR", "ABPSYS", "ABPDIAS", "ABPMEAN", "RESP", "SPO2"]
df_missing_values_columns = [
    "subject_id",
    "number_of_icu_stays",
    "percentage_missing_values",
    "required_signals_available",
]

subject_id_column = df_waveform_exists["subject_id"]
number_of_icu_stays_column = np.zeros(len(subject_id_column), dtype=np.int16)
percentage_missing_values_column = np.full(len(subject_id_column), 100.0, dtype=float)
required_signals_available_column = np.ones(len(subject_id_column), dtype=bool)

for index, row in df_waveform_exists.iterrows():
    time_range_icustay = get_time_range_icustay(row)
    icustay_length_in_seconds = (
        time_range_icustay[1] - time_range_icustay[0]
    ).total_seconds()

    current_user_id = str(row.subject_id).zfill(6)
    wdb_base_path = "https://physionet.org/files/"
    wdb_dir_path = f"mimic3wdb-matched/1.0/p{current_user_id[:2]}/p{current_user_id}/"

    wdb_records = urllib.request.urlopen(wdb_base_path + wdb_dir_path + "RECORDS")

    numerics_files_list = [
        get_record_from_line(line)
        for line in wdb_records.readlines()
        if get_record_from_line(line)[-1] == "n"
    ]

    number_of_icu_stays_column[index] = len(numerics_files_list)

    for record in numerics_files_list:
        try:
            signals, fields = wfdb.rdsamp(record, pn_dir=wdb_dir_path)
        except ValueError:
            print("Error occured while reading waveform: ", record)
            export_csv(
                subject_id_column,
                number_of_icu_stays_column,
                percentage_missing_values_column,
                required_signals_available_column,
                "/data/holthausen/generated_files/missing_values_until_ValueError.csv",
            )

        signals_names_list = [
            re.sub(r"[\s%]", "", item.upper()) for item in fields["sig_name"]
        ]

        signals_exist = all(x in signals_names_list for x in required_signals_list)

        required_signals_available_column[index] = (
            required_signals_available_column[index] and signals_exist
        )

        time_range_record = get_time_range_record(fields)
        record_length_in_seconds = (
            time_range_record[1] - time_range_record[0]
        ).total_seconds()

        if not ranges_overlap(time_range_icustay, time_range_record):
            percentage_missing_values_column[index] -= (
                record_length_in_seconds / icustay_length_in_seconds
            ) * 100

        print(f"Time range record: {time_range_record[0]} --- {time_range_record[1]}")
        print(record_length_in_seconds)
        print(
            f"Time range icustay: {time_range_icustay[0]} --- {time_range_icustay[1]}"
        )
        print(icustay_length_in_seconds)
        print(percentage_missing_values_column[index])

        # if percentage_missing_values_column[index] < 0:
        #     export_csv(
        #         subject_id_column,
        #         number_of_icu_stays_column,
        #         percentage_missing_values_column,
        #         required_signals_available_column,
        #     )
        #     quit()

export_csv(
    subject_id_column,
    number_of_icu_stays_column,
    percentage_missing_values_column,
    required_signals_available_column,
    "/data/holthausen/generated_files/missing_values.csv",
)
