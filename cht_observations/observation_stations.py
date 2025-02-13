from cht_observations._ndbc import Source as ndbc_Source
from cht_observations._noaa_coops import Source as noaa_coops_Source
from cht_observations._station_source import StationSource


def source(name: str) -> StationSource:
    if name == "ndbc":
        return ndbc_Source()
    elif name == "noaa_coops":
        return noaa_coops_Source()
    else:
        raise ValueError(f"Unknown source: {name}")
