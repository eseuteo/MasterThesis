import datetime


def get_record_from_line(line):
    return str(line.decode("utf-8")).rstrip()


def get_signal_index(signal: str, signal_names: [str]):
    return signal_names.index(signal)


def get_time_range_record(fields):
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

    return (record_start_time, record_end_time)


def get_time_range_icustay(row):
    # ###
    # print(str(row["intime"]))
    # print(str(row["outtime"]))
    # print(
    #     (
    #         datetime.datetime.strptime(str(row["intime"]), "%Y-%m-%d %H:%M:%S"),
    #         datetime.datetime.strptime(str(row["outtime"]), "%Y-%m-%d %H:%M:%S"),
    #     )
    # )

    # asd
    # ###
    return (
        datetime.datetime.strptime(str(row["intime"]), "%Y-%m-%d %H:%M:%S"),
        datetime.datetime.strptime(str(row["outtime"]), "%Y-%m-%d %H:%M:%S"),
    )


def get_delta_earliest_end_latest_start(range_a, range_b):
    latest_start = max(range_a[0], range_b[0])
    earliest_end = min(range_a[1], range_b[1])
    return (earliest_end - latest_start).days + 1


def ranges_overlap(range_a, range_b):
    return min(range_a[1], range_b[1]) < max(range_a[0], range_b[0])
