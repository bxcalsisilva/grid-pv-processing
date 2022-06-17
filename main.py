import json
import requests
from datetime import date
import pandas as pd

import time

import schemas
import utils
import filters

config = json.load(open("config.json"))
config = schemas.Configuration(**config)

locations = requests.get(f"{config.api_url}/locations/").json()
locations = [schemas.Location(**dct) for dct in locations]
modules = requests.get(f"{config.api_url}/modules/").json()
modules = [schemas.Module(**dct) for dct in modules]
systems = requests.get(f"{config.api_url}/systems/").json()
systems = [schemas.System(**dct) for dct in systems]

day = date.today()

day = day.date()
start = time.time()

for loc in locations:
    filepath = utils.daq_filepath(config.local_folder, loc.loc, loc.daq, day)
    # print(f"daq {loc.loc} file exists: ", filepath.exists())

    names = config.daq_colnames.__getattribute__(loc.loc)
    df_daq = utils.read_file(filepath, names, day)
    # print(f"daq {loc.loc} length: ", df_daq.__len__())

    irr_colname = config.irr_colname.__getattribute__(loc.loc)
    df_irr = utils.get_variable(df_daq, irr_colname)

    systems = utils.get_systems(config.api_url, loc.loc)
    for sys in systems:
        print(sys.sys)
        filepath = utils.sfcr_filepath(
            config.local_folder, sys.loc, sys.sfcr, sys.mod, day
        )
        # print(f"sfcr {sys.sys} file exists: ", filepath.exists())

        names = config.sfcr_colnames
        df_sfcr = utils.read_file(filepath, names, day)
        # print(f"sfcr {sys.sys} length: ", df_daq.__len__())

        df_tmod_c = utils.get_variable(df_daq, f"t_mod_c_{sys.mod}")
        df_tmod_s = utils.get_variable(df_daq, f"t_mod_s_{sys.mod}")
        df_p_dc = utils.get_variable(df_sfcr, "p_dc")
        df_p_ac = utils.get_variable(df_sfcr, "p_ac")

        df_irr.sort_values(by="dt", inplace=True, ignore_index=True)
        df_tmod_c.sort_values(by="dt", inplace=True, ignore_index=True)
        df_tmod_s.sort_values(by="dt", inplace=True, ignore_index=True)
        df_p_dc.sort_values(by="dt", inplace=True, ignore_index=True)
        df_p_ac.sort_values(by="dt", inplace=True, ignore_index=True)

        df_irr = filters.irradiance(df_irr["dt"], df_irr["val"], day)
        df_tmod = filters.module_temperatures(
            df_tmod_c["dt"],
            df_tmod_c[f"val"],
            df_tmod_s["dt"],
            df_tmod_s[f"val"],
            day,
        )
        df_p_dc = filters.power(df_p_dc["dt"], df_p_dc["val"], sys.p_m, day)
        df_p_ac = filters.power(df_p_ac["dt"], df_p_ac["val"], sys.p_m, day)

        df_irr.dropna(inplace=True)
        df_tmod.dropna(inplace=True)
        df_p_dc.dropna(inplace=True)
        df_p_ac.dropna(inplace=True)

        df_irr.sort_values(by="dt", inplace=True, ignore_index=True)
        df_tmod.sort_values(by="dt", inplace=True, ignore_index=True)
        df_p_dc.sort_values(by="dt", inplace=True, ignore_index=True)
        df_p_ac.sort_values(by="dt", inplace=True, ignore_index=True)

        if filters.corroborate_measurement(df_irr["dt"].tolist(), day):
            df_irr = pd.DataFrame(columns=["dt", "val"])
        if filters.corroborate_measurement(df_tmod["dt"].tolist(), day):
            df_tmod = pd.DataFrame(columns=["dt", "val"])
        if filters.corroborate_measurement(df_p_dc["dt"].tolist(), day):
            df_p_dc = pd.DataFrame(columns=["dt", "val"])
        if filters.corroborate_measurement(df_p_ac["dt"].tolist(), day):
            df_p_ac = pd.DataFrame(columns=["dt", "val"])

        df_joined = pd.DataFrame(columns=["dt"])
        df = df_irr[["dt", "val"]].copy().rename(columns={"val": "irr"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")
        df = df_tmod[["dt", "val"]].copy().rename(columns={"val": "t_mod"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")
        df = df_p_dc[["dt", "val"]].copy().rename(columns={"val": "p_dc"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")
        df = df_p_ac[["dt", "val"]].copy().rename(columns={"val": "p_ac"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")

        df_joined["dt"] = pd.DataFrame(df_joined["dt"])

        try:
            df_joined["dt_hour"] = (
                df_joined["dt"].dt.hour
                + (df_joined["dt"].dt.minute / 60)
                + (df_joined["dt"].dt.second / 3600)
            )
        except AttributeError:
            df_joined["dt_hour"] = None

        number_cols = df_joined.select_dtypes(include="number").columns.tolist()
        df_joined.dropna(inplace=True, subset=number_cols, how="all")
        df_joined.sort_values(by="dt", inplace=True, ignore_index=True)

        # POST results
        utils.post_irr(df_irr, sys.loc, config.api_url)
        utils.post_tmod(df_tmod, sys.sys, config.api_url)
        utils.post_power(df_p_dc, sys.sys, "dc", config.api_url)
        utils.post_power(df_p_dc, sys.sys, "ac", config.api_url)

        h = utils.energy(df_irr["dt"], df_irr["val"])
        e_dc = utils.energy(df_p_dc["dt"], df_p_dc["val"])
        e_ac = utils.energy(df_p_ac["dt"], df_p_ac["val"])

        utils.post_energy(sys.sys, "dc", day, e_dc, config.api_url)
        utils.post_energy(sys.sys, "ac", day, e_ac, config.api_url)

        utils.post_yield(sys.sys, "r", day, h, 1000, config.api_url)
        utils.post_yield(sys.sys, "a", day, e_dc, sys.p_m, config.api_url)
        utils.post_yield(sys.sys, "f", day, e_ac, sys.p_m, config.api_url)

        params = utils.get_params(df_joined, subset=["irr", "p_ac"])
        utils.post_efficiency(
            sys.sys,
            day,
            params["e_dc"],
            params["e_ac"],
            params["h"],
            sys.area,
            config.api_url,
        )
        utils.post_performance_ratio(
            sys.sys,
            day,
            params["h"],
            params["e_dc"],
            params["e_ac"],
            sys.p_m,
            config.api_url,
        )

end = time.time()
print(f"Processing of all systems took {round(end - start, 2)} seconds")
