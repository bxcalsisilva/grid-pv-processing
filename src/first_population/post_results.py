import requests
import pandas as pd
from pathlib import Path


cwd = Path(r"D:\pucp\sfcr_aplicada\processed\joined")

locations = ["pucp", "uni", "untrm", "unaj", "unjbg", "unsa"]
modules = ["perc", "hit", "cigs"]
nominal_powers = [1675, 1650, 1610]

systems = [
    (loc, mod, pm) for loc in locations for mod, pm in zip(modules, nominal_powers)
]

api = "http://ec2-54-232-67-195.sa-east-1.compute.amazonaws.com:8000/processed"

for (loc, mod, pm) in systems:
    print(loc, mod)
    sys = f"{loc}-{mod}"

    filename = f"{loc}_{mod}_params.csv"
    df = pd.read_csv(cwd / filename)

    df = df.round(4)

    type = "dc"

    for type in ["dc", "ac"]:
        df1 = df[["day", f"e_{type}"]].copy()
        df1.rename(columns={f"e_{type}": "val"}, inplace=True)
        dct = df1.to_dict("list")
        dct["sys"] = sys
        dct["type"] = type
        response = requests.post(f"{api}/energies/", json=dct)
        if response.status_code != 200:
            print(f"Problem posting energy {type}")

    df1 = df[["day", "e_dc", "e_ac", "h"]].copy()
    dct = df1.to_dict("list")
    dct["sys"] = sys
    response = requests.post(f"{api}/efficiencies/", json=dct)
    if response.status_code != 200:
        print(f"Problem posting efficiencies")

    df1 = df.copy()
    df1["y_r"] = df1["h"] / 1000
    df1["y_a"] = df1["e_dc"] / pm
    df1["y_f"] = df1["e_ac"] / pm

    for type in ["r", "a", "f"]:
        df2 = df1[["day", f"y_{type}"]].copy()
        df2.rename(columns={f"y_{type}": "val"}, inplace=True)
        df2 = df2.round(4)
        dct = df2.to_dict("list")
        dct["sys"] = sys
        dct["type"] = type
        response = requests.post(f"{api}/yields/", json=dct)
        if response.status_code != 200:
            print(f"Problem posting yields {type}")

    df1 = df.copy()
    df1["y_r"] = df1["h_sync"] / 1000
    df1["y_a"] = df1["e_dc_sync"] / pm
    df1["y_f"] = df1["e_ac_sync"] / pm
    subset = ["y_r", "y_a", "y_f"]
    df1 = df1[["day", "y_r", "y_a", "y_f"]]
    df1.dropna(subset=subset, how="all", inplace=True)
    df1 = df1.round(4)

    dct = df1.to_dict("list")
    dct["sys"] = sys
    response = requests.post(f"{api}/performance_ratios/", json=dct)
    if response.status_code != 200:
        print(f"Problem posting performance_ratios")
