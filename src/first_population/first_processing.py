import datetime
import pandas as pd
from pathlib import Path
import logging

from processing import filters, utils

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
path_folder = Path(r"C:\Users\Brando\Documents\pv-app\data_systemas")
# locations = ["pucp", "uni", "untrm", "unaj", "unjbg", "unsa"]
loc = "unaj"
pyr = "pyr"
start_day = datetime.date(2022, 7, 2)
end_day = datetime.date(2022, 7, 9)

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
    df_params_joined_main = pd.DataFrame(
        columns=["day", "h", "e_dc", "e_ac", "h_sync", "e_dc_sync", "e_ac_sync"]
    )

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

        if df_daq_day.empty:
            print("daq empty")
        if df_sfcr_day.empty:
            print("sfcr empty")

        df_irr = df_daq_day[["dt", pyr]].dropna()
        df_tmod_c = df_daq_day[["dt", f"t_mod_c_{mod}"]].dropna()
        df_tmod_s = df_daq_day[["dt", f"t_mod_s_{mod}"]].dropna()
        df_p_dc = df_sfcr_day[["dt", "p_dc"]].dropna()
        df_p_ac = df_sfcr_day[["dt", "p_ac"]].dropna()

        df_irr.sort_values(by="dt", inplace=True, ignore_index=True)
        df_tmod_c.sort_values(by="dt", inplace=True, ignore_index=True)
        df_tmod_s.sort_values(by="dt", inplace=True, ignore_index=True)
        df_p_dc.sort_values(by="dt", inplace=True, ignore_index=True)
        df_p_ac.sort_values(by="dt", inplace=True, ignore_index=True)

        # print("irr len", df_irr.__len__())
        # print("t_mod_c len", df_tmod_c.__len__())
        # print("t_mod_s len", df_tmod_s.__len__())
        # print("p_dc len", df_p_dc.__len__())
        # print("p_ac len", df_p_ac.__len__())

        df_irr = filters.irradiance(df_irr["dt"], df_irr[pyr], day.date())
        df_tmod = filters.module_temperatures(
            df_tmod_c["dt"],
            df_tmod_c[f"t_mod_c_{mod}"],
            df_tmod_s["dt"],
            df_tmod_s[f"t_mod_s_{mod}"],
            day.date(),
        )
        df_p_dc = filters.power(df_p_dc["dt"], df_p_dc["p_dc"], pm, day.date())
        df_p_ac = filters.power(df_p_ac["dt"], df_p_ac["p_ac"], pm, day.date())

        # print("irr len", df_irr.__len__())
        # print("t_mod len", df_tmod.__len__())
        # print("p_dc len", df_p_dc.__len__())
        # print("p_ac len", df_p_ac.__len__())

        df_irr.dropna(inplace=True)
        df_tmod.dropna(inplace=True)
        df_p_dc.dropna(inplace=True)
        df_p_ac.dropna(inplace=True)

        # print("irr len", df_irr.__len__())
        # print("t_mod len", df_tmod.__len__())
        # print("p_dc len", df_p_dc.__len__())
        # print("p_ac len", df_p_ac.__len__())

        df_irr.sort_values(by="dt", inplace=True, ignore_index=True)
        df_tmod.sort_values(by="dt", inplace=True, ignore_index=True)
        df_p_dc.sort_values(by="dt", inplace=True, ignore_index=True)
        df_p_ac.sort_values(by="dt", inplace=True, ignore_index=True)

        if filters.corroborate_measurement(df_irr["dt"].tolist(), day.date()):
            df_irr = pd.DataFrame(columns=["dt", "val"])
        if filters.corroborate_measurement(df_tmod["dt"].tolist(), day.date()):
            df_tmod = pd.DataFrame(columns=["dt", "val"])
        if filters.corroborate_measurement(df_p_dc["dt"].tolist(), day.date()):
            df_p_dc = pd.DataFrame(columns=["dt", "val"])
        if filters.corroborate_measurement(df_p_ac["dt"].tolist(), day.date()):
            df_p_ac = pd.DataFrame(columns=["dt", "val"])

        # print("irr len", df_irr.__len__())
        # print("t_mod len", df_tmod.__len__())
        # print("p_dc len", df_p_dc.__len__())
        # print("p_ac len", df_p_ac.__len__())

        # join all variables
        df_joined = pd.DataFrame(columns=["dt"])
        df = df_irr[["dt", "val"]].copy().rename(columns={"val": "irr"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")
        df = df_tmod[["dt", "val"]].copy().rename(columns={"val": "t_mod"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")
        df = df_p_dc[["dt", "val"]].copy().rename(columns={"val": "p_dc"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")
        df = df_p_ac[["dt", "val"]].copy().rename(columns={"val": "p_ac"})
        df_joined = pd.merge(df_joined, df, on="dt", how="outer")

        # print("df_joined", df_joined.__len__())

        df_joined["dt"] = pd.DataFrame(df_joined["dt"])

        try:
            df_joined["dt_hour"] = (
                df_joined["dt"].dt.hour
                + (df_joined["dt"].dt.minute / 60)
                + (df_joined["dt"].dt.second / 3600)
            )
        except AttributeError:
            df_joined["dt_hour"] = None

        # print("df_joined", df_joined.__len__())

        number_cols = df_joined.select_dtypes(include="number").columns.tolist()
        df_joined.dropna(inplace=True, subset=number_cols, how="all")
        df_joined.sort_values(by="dt", inplace=True, ignore_index=True)

        # print("df_joined", df_joined.__len__())

        df_joined_main = pd.concat([df_joined_main, df_joined], ignore_index=True)

        # print("df_joined_main", df_joined_main.__len__())

        h = utils.energy(df_irr["dt"], df_irr["val"])
        e_dc = utils.energy(df_p_dc["dt"], df_p_dc["val"])
        e_ac = utils.energy(df_p_ac["dt"], df_p_ac["val"])
        params = utils.get_params(df_joined, subset=["irr", "p_ac"])

        dct = dict(
            day=day.date(),
            h=h,
            e_dc=e_dc,
            e_ac=e_ac,
            h_sync=params["h"],
            e_dc_sync=params["e_dc"],
            e_ac_sync=params["e_ac"],
        )
        df_params = pd.DataFrame(dct, index=[0])
        df_params_joined_main = pd.concat(
            [df_params_joined_main, df_params], ignore_index=True
        )

    # drop rows where there is only nan (number columns)
    number_cols = ["irr", "t_mod", "p_dc", "p_ac", "dt_hour"]
    df_joined_main.dropna(inplace=True, subset=number_cols, how="all")
    df_joined_main.sort_values(by="dt", inplace=True)

    fldr_path = path_folder / "joined"
    fldr_path.mkdir(parents=True, exist_ok=True)
    fl_name = f"{loc}_{mod}_{start_day}_{end_day}_joined.csv"
    df_joined_main.to_csv(fldr_path / fl_name, index=False)
    fl_name = f"{loc}_{mod}_{start_day}_{end_day}_params.csv"
    df_params_joined_main.to_csv(fldr_path / fl_name, index=False)
