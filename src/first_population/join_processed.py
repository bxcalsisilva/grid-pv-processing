from pathlib import Path
import pandas as pd

cwd = Path(r"D:\pucp\sfcr_aplicada\processed")

locations = ["pucp", "uni", "untrm", "unaj", "unjbg", "unsa"]
modules = ["perc", "hit", "cigs"]

systems = [(loc, mod) for loc in locations for mod in modules]

for (loc, mod) in systems:
    print(loc, mod)
    folderpath = cwd / "separated"
    files = folderpath.glob(f"{loc}_{mod}_*_joined.csv")

    df_main = pd.DataFrame()

    for f in files:
        df = pd.read_csv(f)
        df_main = pd.concat([df_main, df], ignore_index=True)

    df_main["dt"] = pd.to_datetime(df_main["dt"], format="%Y-%m-%d %H:%M:%S")
    df_main.sort_values(by="dt", ignore_index=True, inplace=True)

    filepath = cwd / f"{loc}_{mod}_joined.csv"
    df_main.to_csv(filepath, index=False)

    folderpath = cwd / "separated"
    files = folderpath.glob(f"{loc}_{mod}_*_params.csv")

    df_main = pd.DataFrame()

    for f in files:
        df = pd.read_csv(f)
        df_main = pd.concat([df_main, df], ignore_index=True)

    df_main["day"] = pd.to_datetime(df_main["day"], format="%Y-%m-%d")
    df_main.sort_values(by="day", ignore_index=True, inplace=True)

    filepath = cwd / f"{loc}_{mod}_params.csv"
    df_main.to_csv(filepath, index=False)
