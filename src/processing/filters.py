import pandas as pd
from datetime import datetime, date, timedelta, time
import logging


def select_range(df: pd.DataFrame, column: str, lower_limit: float, upper_limit: float):
    df = df[(lower_limit <= df[column]) & (df[column] <= upper_limit)]
    logging.info(f"Selected range filter, new len: {df.__len__()}")
    return df


def derivative(df: pd.DataFrame, column: str):
    derivative = df[column].diff().abs() / df.dt.diff().dt.total_seconds()
    return derivative


def dead_values(
    df: pd.DataFrame, derivative: float, column: str = None, lower_limit: float = None
):
    if lower_limit is not None:
        mask = ~((lower_limit < df[column]) & (derivative > df["derivative"]))
        df = df[mask]
    else:
        mask = ~(derivative > df["derivative"])
        df = df[mask]
    logging.info(f"Dead values filter, new len: {df.__len__()}")
    return df


def abrupt_change(df: pd.DataFrame, upper_limit: float):
    df = df[df["derivative"] <= upper_limit]
    logging.info(f"Abrupt change filter, new len: {df.__len__()}")
    return df


def irradiance(dt: list[datetime], val: list[float], day: date):
    df = pd.DataFrame(dict(dt=dt, val=val))
    df["dt"] = pd.to_datetime(df["dt"])

    df["date"] = day.strftime("%Y-%m-%d")
    df["time"] = df["dt"].dt.strftime("%H:%M:%S")
    df["dt"] = df["date"] + " " + df["time"]

    df["dt"] = pd.to_datetime(df["dt"])

    logging.info(f"Irradiance len: {df.__len__()}")

    if df.empty:
        return pd.DataFrame(columns=["dt", "val"])

    # resample per minute basis
    df = df.resample("1min", on="dt").mean().reset_index()

    df.sort_values(by="dt", inplace=True, ignore_index=True)

    # daylight hours
    df = df[df["val"] >= 20]

    # Filter values outside reasonable bounds
    df = select_range(
        df,
        column="val",
        lower_limit=20,
        upper_limit=2000,
    )

    # df["derivative"] = derivative(df, column="val")
    # df = dead_values(
    #     df,
    #     derivative=0.0001,
    #     column="val",
    #     lower_limit=5,
    # )
    # df = abrupt_change(df, upper_limit=800)

    df.dropna(inplace=True)
    df["val"] = df["val"].round(4)
    return df


def module_temperature(dt: list[datetime], val: list[float], day: date):
    df = pd.DataFrame(dict(dt=dt, val=val))
    df["dt"] = pd.to_datetime(df["dt"])

    logging.info(f"Module temperature len: {df.__len__()}")

    if df.empty:
        return pd.DataFrame(columns=["dt", "val"])

    df["date"] = day.strftime("%Y-%m-%d")
    df["time"] = df["dt"].dt.strftime("%H:%M:%S")
    df["dt"] = df["date"] + " " + df["time"]

    df["dt"] = pd.to_datetime(df["dt"])

    # resample per minute basis
    df = df.resample("1min", on="dt").mean().reset_index()

    df.sort_values(by="dt", inplace=True, ignore_index=True)

    # range
    df = select_range(
        df,
        column="val",
        lower_limit=-30,
        upper_limit=70,
    )

    # df["derivative"] = derivative(df, column="val")
    # df = dead_values(df, derivative=0.0001)
    # df = abrupt_change(df, upper_limit=4)

    df.dropna(inplace=True)
    df["val"] = df["val"].round(4)
    return df


def module_temperatures(
    tmod_c_dt,
    tmod_c_val,
    tmod_s_dt,
    tmod_s_val,
    day: date,
):
    df_tmod_c = module_temperature(tmod_c_dt, tmod_c_val, day)
    df_tmod_s = module_temperature(tmod_s_dt, tmod_s_val, day)

    dts, vals = list(), list()

    dts.extend(df_tmod_c["dt"])
    dts.extend(df_tmod_s["dt"])

    vals.extend(df_tmod_c["val"])
    vals.extend(df_tmod_s["val"])

    df = pd.DataFrame({"dt": dts, "val": vals})

    if df.empty:
        return pd.DataFrame(columns=["dt", "val"])

    df = df.resample("1min", on="dt").mean().reset_index()

    df.dropna(inplace=True)
    df["val"] = df["val"].round(4)

    return df


def power(dt: list[datetime], val: list[float], p_m: float, day: date):
    df = pd.DataFrame(dict(dt=dt, val=val))
    df["dt"] = pd.to_datetime(df["dt"])

    logging.info(f"Power len: {df.__len__()}")

    if df.empty:
        return pd.DataFrame(columns=["dt", "val"])

    df["date"] = day.strftime("%Y-%m-%d")
    df["time"] = df["dt"].dt.strftime("%H:%M:%S")
    df["dt"] = df["date"] + " " + df["time"]

    df["dt"] = pd.to_datetime(df["dt"])

    # resample per minute basis
    df = df.resample("1min", on="dt").mean().reset_index()

    df.sort_values(by="dt", inplace=True, ignore_index=True)

    df = select_range(
        df,
        column="val",
        lower_limit=-0.01 * p_m,
        upper_limit=1.05 * p_m,
    )

    # df["derivative"] = derivative(df, column="val")
    # df = abrupt_change(df, upper_limit=0.8 * p_m)

    df.dropna(inplace=True)
    df["val"] = df["val"].round(4)
    return df


def corroborate_measurement(dts: list[datetime], day: date):
    data = pd.DataFrame(dict(dt=dts))
    data["dt"] = pd.to_datetime(data["dt"])

    start_time = datetime.combine(day, time(10, 30))
    end_time = datetime.combine(day, time(13, 30))

    df = data[data["dt"] < start_time].copy()

    if df.empty:
        # true when there is no values outside the time interval
        return True

    df = data[end_time < data["dt"]].copy()

    if df.empty:
        # true when there is no values outside the time interval
        return True

    # test for interruptions
    try:
        start, end = dts[0], dts[-1]
    except IndexError:
        return True

    minutes = (
        (end - start).total_seconds() / 60
    ) + 1  # plus uno because otherwise it counts interval
    try:
        proportion = len(dts) / minutes
    except ZeroDivisionError:
        return True

    if proportion < 0.8:
        return True

    return False
