from pathlib import Path
import pandas as pd
from datetime import date, timedelta

path_folder = Path(r"C:\Users\xavie\OneDrive\Documentos\pv-app\data_systemas")
sfcr_folder = Path(r"C:\Users\xavie\OneDrive\Documentos\sfcr_aplicada")

loc = "unsa"
systems = ["perc", "hit", "cigs"]

for sfcr, mod in enumerate(systems):
    sfcr += 1
    df_joined = pd.read_csv(path_folder / f"sfcr/sfcr_{loc}_{mod}.csv")
    df_joined["dt"] = pd.to_datetime(df_joined["dt"])
    names = [
        "day",
        "time",
        "i_dc",
        "v_dc",
        "p_dc",
        "i_ac",
        "v_ac",
        "p_ac",
        "f_ac",
        "q_ac",
        "s_ac",
    ]

    max_day = df_joined["dt"].dt.date.max()
    today = date.today()

    for day in pd.date_range(max_day + timedelta(days=1), today):
        print(day)
        filepath = sfcr_folder / day.strftime(
            f"{loc.upper()}/%Y/%Y_%m/SFCR{sfcr}-{mod.upper()}-{loc.upper()}_%Y_%m_%d.csv"
        )
        if not filepath.exists():
            print(filepath, "doesn't exists")
            continue
        df = pd.read_csv(filepath, sep=";", header=None, names=names, index_col=None)
        df["dt"] = df["day"] + " " + df["time"]
        df["dt"] = pd.to_datetime(df["dt"], format="%d/%m/%Y %H:%M:%S")
        df.drop(["day", "time"], axis=1, inplace=True)
        dt_col = df.pop("dt")
        df.insert(0, "dt", dt_col)

        df_joined = pd.concat([df_joined, df], ignore_index=True)
    df_joined.to_csv(path_folder / f"sfcr/sfcr_{loc}_{mod}.csv", index=False)
