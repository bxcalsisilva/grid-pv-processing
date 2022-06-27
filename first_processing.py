import datetime
import pandas as pd
from pathlib import Path
import logging

import filters

logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.basicConfig(
    filename="info.log",
    filemode="a",
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    encoding="utf-8",
)

logging.info("Program started")

# Configuration
path_folder = Path("/home/bcalsi/Documents/data_systemas")
# locations = ["pucp", "uni", "untrm", "unaj", "unjbg", "unsa"]
loc = "unaj"
pyr = "mini_1"
start_day = datetime.date(2022, 1, 1)
end_day = datetime.date(2022, 1, 31)

modules = ["perc", "hit", "cigs"]
pms = [1675, 1650, 1610]
gammas = [-0.0037, -0.00258, -0.0023]

df_daq = pd.read_csv(path_folder / f"daq/daq_{loc}.csv")
df_daq["dt"] = pd.to_datetime(df_daq["dt"])
df_daq = (
    df_daq.groupby(pd.Grouper(key="dt", freq="min"))
    .mean()
    .dropna(how="all")
    .reset_index(drop=False)
)
df_daq.sort_values(by="dt", inplace=True)

for (mod, pm, gamma) in zip(modules, pms, gammas):
    print(loc, mod)
    logging.info(f"{loc} {mod}")

    df_sfcr = pd.read_csv(path_folder / f"sfcr/sfcr_{loc}_{mod}.csv")
    df_sfcr["dt"] = pd.to_datetime(df_sfcr["dt"])
    df_sfcr = (
        df_sfcr.groupby(pd.Grouper(key="dt", freq="min"))
        .mean()
        .dropna(how="all")
        .reset_index(drop=False)
    )
    df_sfcr.sort_values(by="dt", inplace=True)

    df_joined_main = pd.DataFrame()

    dates = pd.date_range(start=start_day, end=end_day)

    for day in dates:
        print(day)
        logging.info(f"{day}")
        next_day = day + datetime.timedelta(days=1)

        mask = (day <= df_daq["dt"]) & (df_daq["dt"] < next_day)
        df_daq_day = df_daq[mask]

        mask = (day <= df_sfcr["dt"]) & (df_sfcr["dt"] < next_day)
        df_sfcr_day = df_sfcr[mask]

        df_daq_day = df_daq_day.sort_values(by="dt", ignore_index=True)
        df_sfcr_day = df_sfcr_day.sort_values(by="dt", ignore_index=True)

        irr = df_daq_day[["dt", pyr]].dropna()
        t_mod_center = df_daq_day[["dt", f"t_mod_c_{mod}"]].dropna()
        t_mod_side = df_daq_day[["dt", f"t_mod_s_{mod}"]].dropna()
        p_dc = df_sfcr_day[["dt", "p_dc"]].dropna()
        p_ac = df_sfcr_day[["dt", "p_ac"]].dropna()

        irr.sort_values(by="dt", inplace=True)
        t_mod_center.sort_values(by="dt", inplace=True)
        t_mod_side.sort_values(by="dt", inplace=True)
        p_dc.sort_values(by="dt", inplace=True)
        p_ac.sort_values(by="dt", inplace=True)

        irr = filters.irradiance(irr["dt"], irr[pyr], day.date())
        t_mod = filters.module_temperatures(
            t_mod_center["dt"],
            t_mod_center[f"t_mod_c_{mod}"],
            t_mod_side["dt"],
            t_mod_side[f"t_mod_s_{mod}"],
            day.date(),
        )
        p_dc = filters.power(p_dc["dt"], p_dc["p_dc"], pm, day.date())
        p_ac = filters.power(p_ac["dt"], p_ac["p_ac"], pm, day.date())

        irr.dropna().sort_values(by="dt", inplace=True)
        t_mod_center.dropna().sort_values(by="dt", inplace=True)
        t_mod_side.dropna().sort_values(by="dt", inplace=True)
        p_dc.dropna().sort_values(by="dt", inplace=True)
        p_ac.dropna().sort_values(by="dt", inplace=True)

        if filters.corroborate_measurement(irr["dt"].tolist(), day):
            logging.info(f"Corroborate measurement filter irr not passed")
            irr = pd.DataFrame(columns=["dt", "val"])
        if filters.corroborate_measurement(t_mod["dt"].tolist(), day):
            logging.info(f"Corroborate measurement filter tmod not passed")
            t_mod = pd.DataFrame(columns=["dt", "val"])
        if filters.corroborate_measurement(p_dc["dt"].tolist(), day):
            logging.info(f"Corroborate measurement filter p_dc not passed")
            p_dc = pd.DataFrame(columns=["dt", "val"])
        if filters.corroborate_measurement(p_ac["dt"].tolist(), day):
            logging.info(f"Corroborate measurement filter p_ac not passed")
            p_ac = pd.DataFrame(columns=["dt", "val"])

        # join all variables
        df_joined = pd.DataFrame(columns=["dt"])
        df = irr.rename(columns={"val": "irr"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")
        df = t_mod.rename(columns={"val": "t_mod"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")
        df = p_dc.rename(columns={"val": "p_dc"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")
        df = p_ac.rename(columns={"val": "p_ac"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")

        df_joined.sort_values(by="dt", inplace=True)
        df_joined_main = pd.concat([df_joined_main, df_joined], ignore_index=True)

    # drop rows where there is only nan (number columns)
    number_cols = df_joined_main.select_dtypes(include="number").columns.tolist()
    df_joined_main.dropna(inplace=True, subset=number_cols, how="all")

    df_joined_main.sort_values(by="dt", inplace=True)

    fldr_path = path_folder / "joined"
    fldr_path.mkdir(parents=True, exist_ok=True)
    fl_name = f"{loc}_{mod}_{start_day}_{end_day}_joined.csv"
    df_joined_main.to_csv(fldr_path / fl_name, index=False)
