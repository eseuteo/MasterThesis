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

REQUIRED_SIGNALS = ["HR", "ABPSYS", "ABPDIAS", "ABPMEAN", "RESP", "SPO2"]


def get_empty_signals_dict(length, signals_list, time_range_icustay):
    signals_dict = dict()
    basetime = time_range_icustay[0]
    if sampling_frequency_magnitude == "seconds":
        time_column = np.array(
            [basetime + datetime.timedelta(seconds=i + 1) for i in range(signal_length)]
        )
    else:
        time_column = np.array(
            [basetime + datetime.timedelta(minutes=i + 1) for i in range(signal_length)]
        )
    signals_dict["TIME"] = time_column
    for item in signals_list:
        signals_dict[item] = np.full(length, np.nan, dtype=float)
    return signals_dict


def update_signals_dict(signals, signals_dict, signals_names_list, index_start):
    index_dict = dict(
        [
            (item, get_signal_index(item, signals_names_list))
            for item in REQUIRED_SIGNALS
        ]
    )
    for item in REQUIRED_SIGNALS:
        signal_length = signals[:, index_dict[item]].shape[0]
        icustay_length = signals_dict[item].shape[0]
        index_end = min(index_start + signal_length, icustay_length)
        signal_info = signals[:, index_dict[item]]
        signals_dict[item][index_start:index_end] = signal_info[
            : index_end - index_start
        ]


def get_index_difference(point_in_time_a, point_in_time_b, magnitude):
    return int(
        (point_in_time_b - point_in_time_a).total_seconds()
        // (1 if magnitude == "seconds" else 60)
    )


def get_overlapped_range(range_icustay, range_record):
    latest_start = max(range_icustay[0], range_record[0])
    earliest_end = min(range_icustay[1], range_record[1])
    delta = (earliest_end - latest_start).days + 1
    overlap = max(0, delta)
    range_output = None
    if overlap:
        range_output = (latest_start, earliest_end)
    return range_output


def extract_ts_from_subject(subject_id: int):
    signals_dict = None

    current_user_id = str(subject_id).zfill(6)
    wdb_path = f"https://physionet.org/files/mimic3wdb-matched/1.0/p{current_user_id[:2]}/p{current_user_id}/"

    connection = psycopg2.connect(database="mimic", user=os.environ["USERNAME"])
    cursor = connection.cursor()
    cursor.execute("set search_path to mimiciii")

    query_subject_id = (
        f"SELECT  * FROM sepsis3_cohort coh WHERE coh.subject_id = {subject_id}"
    )
    df_subject_id = pd.read_sql(query_subject_id, connection)

    time_range_icustay = get_time_range_icustay(df_subject_id.iloc[0])
    icustay_length_in_seconds = (
        time_range_icustay[1] - time_range_icustay[0]
    ).total_seconds()

    wdb_records = urllib.request.urlopen(wdb_path + "RECORDS")

    numerics_files_list = [
        get_record_from_line(line)
        for line in wdb_records.readlines()
        if get_record_from_line(line)[-1] == "n"
    ]

    for record in numerics_files_list:
        try:
            signals, fields = wfdb.rdsamp(record, pn_dir=wdb_dir_path)
        except ValueError:
            print(f"Error occurred while reading waveform: {record}")
            continue

        time_range_record = get_time_range_record(fields)
        time_range_overlapped = get_overlapped_range(
            time_range_icustay, time_range_record
        )

        if time_range_overlapped is None:
            continue

        sampling_frequency_magnitude = (
            "seconds" if "%.3f" % (fields["fs"]) == "1.000" else "minutes"
        )

        if signals_dict is None:
            signal_length = icustay_length_in_seconds
            if sampling_frequency_magnitude == "minutes":
                signal_length //= 60
            signals_dict = get_empty_signals_dict(signal_length, time_range_icustay)

        signals_names_list = [
            re.sub(r"[\s%]", "", item.upper()) for item in fields["sig_name"]
        ]

        signals_exist = all(x in signals_names_list for x in REQUIRED_SIGNALS)

        index_start_signal = get_index_difference(
            time_range_icustay[0],
            time_range_overlapped[0],
            sampling_frequency_magnitude,
        )

        if signals_exist:
            update_signals_dict(
                signals,
                signals_dict,
                signals_names_list,
                index_start_signal,
            )

    df = pd.DataFrame(signals_dict)
    df.to_csv(f"../../generated_files/ts/signals_{subject_id}.csv", index=False)
