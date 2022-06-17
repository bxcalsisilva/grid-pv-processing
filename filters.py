import pandas as pd
from datetime import datetime, date, timedelta, time


def select_range(df: pd.DataFrame, column: str, lower_limit: float, upper_limit: float):
    df = df[(lower_limit <= df[column]) & (df[column] <= upper_limit)]
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
    return df


def abrupt_change(df: pd.DataFrame, upper_limit: float):
    df = df[df["derivative"] <= upper_limit]

    return df


def irradiance(dt: list[datetime], val: list[float], day: date):
    data = pd.DataFrame(dict(dt=dt, val=val))
    data["dt"] = pd.to_datetime(data["dt"])

    data["date"] = day.strftime("%Y-%m-%d")
    data["time"] = data["dt"].dt.strftime("%H:%M:%S")
    data["dt"] = data["date"] + " " + data["time"]

    data["dt"] = pd.to_datetime(data["dt"])

    if data.empty:
        return pd.DataFrame(columns=["dt", "val"])

    # resample per minute basis
    data = data.resample("1min", on="dt").mean().reset_index()

    data.sort_values(by="dt", inplace=True, ignore_index=True)

    # daylight hours
    data = data[data["val"] >= 20]

    # Filter values outside reasonable bounds
    data = select_range(
        data,
        column="val",
        lower_limit=-6,
        upper_limit=1500,
    )

    data["derivative"] = derivative(data, column="val")
    data = dead_values(
        data,
        derivative=0.0001,
        column="val",
        lower_limit=5,
    )
    data = abrupt_change(data, upper_limit=800)

    data.dropna(inplace=True)
    data["val"] = data["val"].round(4)
    return data


def module_temperature(dt: list[datetime], val: list[float], day: date):
    data = pd.DataFrame(dict(dt=dt, val=val))
    data["dt"] = pd.to_datetime(data["dt"])

    if data.empty:
        return pd.DataFrame(columns=["dt", "val"])

    data["date"] = day.strftime("%Y-%m-%d")
    data["time"] = data["dt"].dt.strftime("%H:%M:%S")
    data["dt"] = data["date"] + " " + data["time"]

    data["dt"] = pd.to_datetime(data["dt"])

    # resample per minute basis
    data = data.resample("1min", on="dt").mean().reset_index()

    data.sort_values(by="dt", inplace=True, ignore_index=True)

    # range
    data = select_range(
        data,
        column="val",
        lower_limit=-30,
        upper_limit=50,
    )

    data["derivative"] = derivative(data, column="val")
    data = dead_values(data, derivative=0.0001)
    data = abrupt_change(data, upper_limit=4)

    data.dropna(inplace=True)
    data["val"] = data["val"].round(4)
    return data


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
    data = pd.DataFrame(dict(dt=dt, val=val))
    data["dt"] = pd.to_datetime(data["dt"])

    if data.empty:
        return pd.DataFrame(columns=["dt", "val"])

    data["date"] = day.strftime("%Y-%m-%d")
    data["time"] = data["dt"].dt.strftime("%H:%M:%S")
    data["dt"] = data["date"] + " " + data["time"]

    data["dt"] = pd.to_datetime(data["dt"])

    # resample per minute basis
    data = data.resample("1min", on="dt").mean().reset_index()

    data.sort_values(by="dt", inplace=True, ignore_index=True)

    data = select_range(
        data,
        column="val",
        lower_limit=-0.01 * p_m,
        upper_limit=1.2 * p_m,
    )

    data["derivative"] = derivative(data, column="val")
    data = abrupt_change(data, upper_limit=0.8 * p_m)

    data.dropna(inplace=True)
    data["val"] = data["val"].round(4)
    return data


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
