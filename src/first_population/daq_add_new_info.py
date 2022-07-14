from pathlib import Path
import pandas as pd
from datetime import date, timedelta

path_folder = Path(r"C:\Users\xavie\OneDrive\Documentos\pv-app\data_systemas")
sfcr_folder = Path(r"C:\Users\xavie\OneDrive\Documentos\sfcr_aplicada")

loc = "pucp"
daq = "DAQ-MS80S"

daq_joined = pd.read_csv(path_folder / f"daq/daq_{loc}.csv")
daq_joined["dt"] = pd.to_datetime(daq_joined["dt"])

daq_columns = daq_joined.columns.tolist()
names = [
    "day",
    "time",
    "kip",
    "mini_1",
    "mini_2",
    "disc_1",
    "t_mod_c_cigs",
    "t_mod_s_cigs",
    "t_mod_c_perc",
    "t_mod_s_perc",
    "t_mod_c_hit",
    "t_mod_s_hit",
    "pyr",
]

max_day = daq_joined["dt"].dt.date.max()
today = date.today()
# today = date(2022, 6, 18)

print(max_day)

for day in pd.date_range(max_day + timedelta(days=1), today):
    print(day)
    filepath_daq = sfcr_folder / day.strftime(
        f"{loc.upper()}/%Y/%Y_%m/{daq}-{loc.upper()}_%Y_%m_%d.csv"
    )
    if not filepath_daq.exists():
        print(filepath_daq, "doesn't exists")
        continue
    df = pd.read_csv(filepath_daq, sep=";", header=None, names=names, index_col=None)
    df["dt"] = df["day"] + " " + df["time"]
    df["dt"] = pd.to_datetime(df["dt"], format="%d/%m/%Y %H:%M:%S")
    df.drop(["day", "time"], axis=1, inplace=True)
    dt_col = df.pop("dt")
    df.insert(0, "dt", dt_col)

    daq_joined = pd.concat([daq_joined, df], ignore_index=True)
daq_joined.to_csv(path_folder / f"daq/daq_{loc}.csv", index=False)
