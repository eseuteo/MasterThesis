import pandas as pd

from datetime import datetime

df_sofa = pd.read_csv("data/sofa_data.csv", dtype={'SOFA_SCORE': 'float32'})

for name, group in df_sofa.groupby("SUBJECT_ID"):
    if name < 58449:
        continue
    try:
        df_signals = pd.read_csv(f"data/ts/signals_{name}.csv")
    except:
        continue
    print(name)
    df_merged = pd.merge(
        df_signals, group[["CALCULATION_TIME", "SOFA_SCORE"]], left_on="TIME", right_on="CALCULATION_TIME", how="left"
    )
    print(df_merged.dtypes)
    first_timestamp = datetime.strptime(df_signals.iloc[0]["TIME"], "%Y-%m-%d %H:%M:%S")
    second_timestamp = datetime.strptime(
        df_signals.iloc[1]["TIME"], "%Y-%m-%d %H:%M:%S"
    )

    signal_length = int(3600 / (second_timestamp - first_timestamp).total_seconds())

    last_valid_sofa_index = df_merged["SOFA_SCORE"].last_valid_index()
    df_merged.loc[
        : last_valid_sofa_index + signal_length, "SOFA_SCORE"
    ] = df_merged.loc[: last_valid_sofa_index + signal_length, "SOFA_SCORE"].fillna(
        method="ffill"
    )

    df_merged[
        ["TIME", "HR", "ABPSYS", "ABPDIAS", "ABPMEAN", "RESP", "SPO2", "SOFA_SCORE"]
    ].to_csv(f"data/ts_sofa/signals_sofa_{name}.csv")
