import json
import requests
from datetime import date
from pathlib import Path
import pandas as pd

import schemas

config = json.load(open("config.json"))
config = schemas.Configuration(**config)

locations = requests.get(f"{config.api_url}/locations/").json()
locations = [schemas.Location(**dct) for dct in locations]
modules = requests.get(f"{config.api_url}/modules/").json()
modules = [schemas.Module(**dct) for dct in modules]
systems = requests.get(f"{config.api_url}/systems/").json()
systems = [schemas.System(**dct) for dct in systems]

day = date(2022, 5, 18)


def get_systems(loc: str):
    url = f"{config.api_url}/locations/{loc}/systems"
    response = requests.get(url).json()
    systems = [schemas.System(**dct) for dct in response]
    return systems


def subfolder_path(folder_path: Path, loc: str, day: date):
    loc = loc.upper()
    subfolder = folder_path / day.strftime(f"{loc}/%Y/%Y_%m")
    return subfolder


def sfcr_filename(sfcr: str, mod: str, loc: str, day: date):
    sfcr = sfcr.upper()
    mod = mod.upper()
    loc = loc.upper()
    day = day.strftime("%Y_%m_%d")

    filename = f"{sfcr}-{mod}-{loc}_{day}.csv"
    return filename


def daq_filename(daq: str, loc: str, day: date):
    daq = daq.upper()
    loc = loc.upper()
    day = day.strftime("%Y_%m_%d")

    filename = f"{daq}-{loc}_{day}.csv"
    return filename


def daq_filepath(local_folder: Path, loc: str, daq: str, day: date):
    folder_path = subfolder_path(local_folder, loc, day)
    filename = daq_filename(daq, loc, day)
    filepath = folder_path / filename

    return filepath


def sfcr_filepath(local_folder: Path, loc: str, sfcr: str, mod: str, day: date):
    folder_path = subfolder_path(local_folder, loc, day)
    filename = sfcr_filename(sfcr, mod, loc, day)
    filepath = folder_path / filename
    return filepath


def read_file(filepath: Path, names: list[str], day: date):
    try:
        df = pd.read_csv(filepath, sep=";", names=names)

        day = day.strftime("%Y-%m-%d")
        df["day"] = day

        df["dt"] = df["day"] + " " + df["time"]
        df["dt"] = pd.to_datetime(df["dt"])

        df.drop(labels=["day", "time"], axis="columns", inplace=True)

        grouper = pd.Grouper(key="dt", freq="min")
        df = df.groupby(grouper).mean()

        df.reset_index(inplace=True)

    except FileNotFoundError:
        df = pd.DataFrame(columns=names)
        df["dt"] = df["day"] + " " + df["time"]
        df.drop(labels=["day", "time"], axis="columns", inplace=True)

    return df


def get_variable(df: pd.DataFrame, colname):
    df = df[["dt", colname]].copy()
    df.rename({colname: "val"}, axis="columns", inplace=True)
    df.dropna(inplace=True)

    return df


for loc in locations:
    filepath = daq_filepath(config.local_folder, loc.loc, loc.daq, day)
    # print(f"daq {loc.loc} file exists: ", filepath.exists())

    names = config.daq_colnames.__getattribute__(loc.loc)
    df_daq = read_file(filepath, names, day)
    # print(f"daq {loc.loc} length: ", df_daq.__len__())

    irr_colname = config.irr_colname.__getattribute__(loc.loc)
    df_irr = get_variable(df_daq, irr_colname)

    systems = get_systems(loc.loc)
    for sys in systems:
        filepath = sfcr_filepath(config.local_folder, sys.loc, sys.sfcr, sys.mod, day)
        # print(f"sfcr {sys.sys} file exists: ", filepath.exists())

        names = config.sfcr_colnames
        df_sfcr = read_file(filepath, names, day)
        # print(f"sfcr {sys.sys} length: ", df_daq.__len__())

        # print(df_sfcr)
        df_tmod_c = get_variable(df_daq, f"t_mod_c_{sys.mod}")
        df_tmod_s = get_variable(df_daq, f"t_mod_s_{sys.mod}")
        df_p_dc = get_variable(df_sfcr, "p_dc")
        df_p_ac = get_variable(df_sfcr, "p_ac")

        print(df_p_ac)

    #     break
    # break
