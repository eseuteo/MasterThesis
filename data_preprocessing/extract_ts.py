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
)

REQUIRED_SIGNALS = ["HR", "ABPSYS", "ABPDIAS", "ABPMEAN", "RESP", "SPO2"]


def get_empty_signals_dict(length, signals_list, time_range_icustay, magnitude):
    signals_dict = dict()
    basetime = time_range_icustay[0]
    if magnitude == "seconds":
        time_column = np.array(
            [basetime + datetime.timedelta(seconds=i + 1) for i in range(length)]
        )
    else:
        time_column = np.array(
            [basetime + datetime.timedelta(minutes=i + 1) for i in range(length)]
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


def extract_ts_from_subject(subject_row):
    # get subject id
    subject_id = subject_row["subject_id"]
    print(subject_id)
    # create signals dict as None
    signals_dict = None

    # get url for retrieving signals
    current_user_id = str(subject_id).zfill(6)
    wdb_base_path = "https://physionet.org/files/"
    wdb_dir_path = f"mimic3wdb-matched/1.0/p{current_user_id[:2]}/p{current_user_id}/"

    # get length (in seconds) of icustay
    time_range_icustay = get_time_range_icustay(subject_row)
    icustay_length_in_seconds = int(
        (time_range_icustay[1] - time_range_icustay[0]).total_seconds()
    )

    # get list of files
    wdb_records = urllib.request.urlopen(wdb_base_path + wdb_dir_path + "RECORDS")

    # filter numeric files
    numerics_files_list = [
        get_record_from_line(line)
        for line in wdb_records.readlines()
        if get_record_from_line(line)[-1] == "n"
    ]

    sampling_frequency_magnitude = None
    prev_sampling_frequency_magnitude = None

    # iterate over numeric files
    for record in numerics_files_list:
        try:
            signals, fields = wfdb.rdsamp(record, pn_dir=wdb_dir_path)
        except ValueError:
            print(f"Error occurred while reading waveform: {record}")
            continue

        # we already have the icu time range, now get the record time range
        time_range_record = get_time_range_record(fields)

        # get the overlapped portion between icustay and record
        time_range_overlapped = get_overlapped_range(
            time_range_icustay, time_range_record
        )

        # skip iteration if not overlapped
        if time_range_overlapped is None:
            continue

        if sampling_frequency_magnitude is not None:
            prev_sampling_frequency_magnitude = sampling_frequency_magnitude
        sampling_frequency_magnitude = (
            "seconds" if "%.3f" % (fields["fs"]) == "1.000" else "minutes"
        )

        if (
            prev_sampling_frequency_magnitude is not None
            and prev_sampling_frequency_magnitude != sampling_frequency_magnitude
        ):
            sampling_frequency_magnitude = prev_sampling_frequency_magnitude
            prev_sampling_frequency_magnitude = None
            continue

        # if signals dict has not been initialized
        if signals_dict is None:
            signal_length = icustay_length_in_seconds
            if sampling_frequency_magnitude == "minutes":
                signal_length //= 60
            signals_dict = get_empty_signals_dict(
                signal_length,
                REQUIRED_SIGNALS,
                time_range_icustay,
                sampling_frequency_magnitude,
            )

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
    path = "/home/ricardohb/Documents/generated_files/ts/"
    df.to_csv(f"{path}signals_{subject_id}.csv", index=False)
