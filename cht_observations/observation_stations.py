"""Factory function for constructing observation station source objects."""

from cht_observations._ndbc import NDBCSource
from cht_observations._noaa_coops import NOAASource
from cht_observations._station_source import StationSource


def source(name: str) -> StationSource:
    """Return a station source object for the given provider name.

    Parameters
    ----------
    name : str
        Data source identifier. Supported values:

        * ``"ndbc"`` — NOAA National Data Buoy Center
        * ``"noaa_coops"`` — NOAA Center for Operational Oceanographic
          Products and Services

    Returns
    -------
    StationSource
        Concrete station source instance for the requested provider.

    Raises
    ------
    ValueError
        If *name* does not match a known data source.
    """
    if name == "ndbc":
        return NDBCSource()
    elif name == "noaa_coops":
        return NOAASource()
    else:
        raise ValueError(f"Unknown source: {name}")
