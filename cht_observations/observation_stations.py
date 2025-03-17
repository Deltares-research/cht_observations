from cht_observations._ndbc import NDBCSource
from cht_observations._noaa_coops import NOAASource
from cht_observations._station_source import StationSource


def source(name: str) -> StationSource:
    if name == "ndbc":
        return NDBCSource()
    elif name == "noaa_coops":
        return NOAASource()
    else:
        raise ValueError(f"Unknown source: {name}")
