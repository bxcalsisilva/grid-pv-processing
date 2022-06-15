from pydantic import BaseModel
from datetime import date
from pathlib import Path


class DaqColnames(BaseModel):
    pucp: list[str]
    uni: list[str]
    untrm: list[str]
    unaj: list[str]
    unjbg: list[str]
    unsa: list[str]


class IrrColname(BaseModel):
    pucp: str
    uni: str
    untrm: str
    unaj: str
    unjbg: str
    unsa: str


class Configuration(BaseModel):
    api_url: str
    local_folder: Path
    irr_colname: IrrColname
    sfcr_colnames: list[str]
    daq_colnames: DaqColnames


class Location(BaseModel):
    loc: str
    region: str
    city: str
    label: str
    daq: str
    latitude: float
    longitude: float
    altitude: float


class Module(BaseModel):
    mod: str
    technology: str
    area: float
    p_m: float
    efficiency: float
    alpha: float
    beta: float
    gamma: float
    noct: float


class System(BaseModel):
    sys: str
    loc: str
    mod: str
    sfcr: str
    p_m: float
    series: int
    parallel: int
    modules: int
    area: float
    commisioned: date
    inclination: float
    orientation: str
    azimuth: float
