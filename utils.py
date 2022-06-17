import requests
from datetime import date, datetime
from pathlib import Path
import pandas as pd
from scipy.integrate import trapezoid

import schemas


def get_systems(api_url: str, loc: str):
    url = f"{api_url}/locations/{loc}/systems"
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


def post_irr(df, loc: str, api_url: str):
    df = df[["dt", "val"]].copy().dropna()
    if df.empty:
        print(f"Irradiances empty")
        return
    df.sort_values("dt", ignore_index=True, inplace=True)
    df["dt"] = df["dt"].astype(str)
    dct = df.to_dict("list")
    dct["loc"] = loc
    response = requests.post(f"{api_url}/irradiances/", json=dct)

    if response.status_code != 200:
        print(f"Irradiances post response {response.status_code}")


def post_tmod(df, sys: str, api_url: str):
    df = df[["dt", "val"]].copy().dropna()
    if df.empty:
        print(f"Irradiances empty")
        return
    df.sort_values("dt", ignore_index=True, inplace=True)
    df["dt"] = df["dt"].astype(str)
    dct = df.to_dict("list")
    dct["sys"] = sys
    response = requests.post(f"{api_url}/module_temperatures/", json=dct)

    if response.status_code != 200:
        print(f"Temp Mod post response {response.status_code}")


def post_power(df, sys: str, typ: str, api_url: str):
    df = df[["dt", "val"]].copy().dropna()
    if df.empty:
        print(f"Irradiances empty")
        return
    df.sort_values("dt", ignore_index=True, inplace=True)
    df["dt"] = df["dt"].astype(str)
    dct = df.to_dict("list")
    dct["sys"] = sys
    dct["type"] = typ
    response = requests.post(f"{api_url}/powers/", json=dct)

    if response.status_code != 200:
        print(f"Powers post response {response.status_code}")


def energy(dt: list[datetime], val: list[float]):
    data = dict(dt=dt, val=val)
    df = pd.DataFrame(data)
    df["dt"] = pd.to_datetime(df["dt"])
    df["val"] = df["val"].astype(float)

    df.dropna(inplace=True)
    df.sort_values("dt", ignore_index=True)
    df["dt_hours"] = (
        df["dt"].dt.hour + (df["dt"].dt.minute / 60) + (df["dt"].dt.second / 3600)
    )
    e = trapezoid(y=df["val"], x=df["dt_hours"])

    return e


def get_params(
    df: pd.DataFrame(columns=["dt", "irr", "p_dc", "p_ac"]), subset: list[str] = list()
):
    if subset:
        df = df.dropna(subset=subset)
    df = df.reset_index(drop=True)

    if df.empty:
        return dict(day=None, h=None, e_dc=None, e_ac=None)

    h = energy(dt=df["dt"], val=df["irr"]).round(4)
    e_dc = energy(dt=df["dt"], val=df["p_dc"]).round(4)
    e_ac = energy(dt=df["dt"], val=df["p_ac"]).round(4)

    day = df.loc[0, "dt"].date()

    return dict(day=day, h=h, e_dc=e_dc, e_ac=e_ac)


def post_energy(sys: str, typ: str, day: date, energy: float, api_url: str):
    day = day.strftime("%Y-%m-%d")
    if energy is None:
        return
    energy = round(energy, 4)
    json = dict(sys=sys, type=typ, day=[day], val=[energy])
    response = requests.post(f"{api_url}/energies/", json=json)

    if response.status_code != 200:
        print(f"Energy {typ} POST response {response.status_code}")


def post_yield(sys: str, typ: str, day: date, e: float, nominal: float, api_url: str):
    day = day.strftime("%Y-%m-%d")
    if e is None:
        return
    yld = e / nominal
    val = round(yld, 4)
    json = dict(sys=sys, type=typ, day=[day], val=[val])
    response = requests.post(f"{api_url}/yields/", json=json)

    if response.status_code != 200:
        print(f"Yield {typ} POST response {response.status_code}")


def post_efficiency(
    sys: str, day: date, e_dc: float, e_ac: float, h: float, area: float, api_url: str
):
    day = day.strftime("%Y-%m-%d")
    if e_dc is not None:
        e_dc = round(e_dc, 4)
    if e_ac is not None:
        e_ac = round(e_ac, 4)
    if h is not None:
        h_area = round(h * area, 4)
    else:
        h_area = h

    if (e_dc is None) & (e_ac is None) & (h_area is None):
        print(day, "efficiency values are all None")
        return
    json = dict(sys=sys, day=[day], e_dc=[e_dc], e_ac=[e_ac], h=[h_area])
    response = requests.post(f"{api_url}/efficiencies/", json=json)

    if response.status_code != 200:
        print(f"Efficiency POST response {response.status_code}")


def post_performance_ratio(
    sys: str, day: date, h: float, e_dc: float, e_ac: float, p_m: float, api_url: str
):
    day = day.strftime("%Y-%m-%d")
    if h is not None:
        y_r = h / 1000
        y_r = round(y_r, 4)
    else:
        y_r = None
    if e_dc is not None:
        y_a = e_dc / p_m
        y_a = round(y_a, 4)
    else:
        y_a = None
    if e_ac is not None:
        y_f = e_ac / p_m
        y_f = round(y_f, 4)
    else:
        y_f = None
    if (y_r is None) & (y_a is None) & (y_f is None):
        print(day, "PR values are all None")
        return
    json = dict(sys=sys, day=[day], y_r=[y_r], y_a=[y_a], y_f=[y_f])
    response = requests.post(f"{api_url}/performance_ratios/", json=json)
    if response.status_code != 200:
        print(f"PR POST response {response.status_code}")
