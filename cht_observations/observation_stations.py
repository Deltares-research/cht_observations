"""Factory function for constructing observation station source objects."""

from typing import Any

from cht_observations._copernicus import CopernicusSource
from cht_observations._emodnet import EMODnetSource
from cht_observations._gesla import GESLASource
from cht_observations._grdc import GRDCSource
from cht_observations._ioc import IOCSource
from cht_observations._ndbc import NDBCSource
from cht_observations._noaa_coops import NOAASource
from cht_observations._station_source import StationSource
from cht_observations._usgs import USGSSource
from cht_observations._waterinfo import WaterinfoSource


def source(name: str, **kwargs: Any) -> StationSource:
    """Return a station source object for the given provider name.

    Parameters
    ----------
    name : str
        Data source identifier. Supported values:

        * ``"ndbc"`` — NOAA National Data Buoy Center
        * ``"noaa_coops"`` — NOAA Center for Operational Oceanographic
          Products and Services
        * ``"ioc"`` — IOC Sea Level Station Monitoring Facility (global)
        * ``"usgs"`` — USGS NWIS (US rivers / streamflow)
        * ``"waterinfo"`` — Rijkswaterstaat / DDL (Netherlands)
        * ``"emodnet"`` — EMODnet Physics (European marine in-situ)
        * ``"copernicus"`` — Copernicus Marine in-situ (auth required)
        * ``"gesla"`` — GESLA historic tide-gauge archive (local files)
        * ``"grdc"`` — GRDC Global Runoff Data Centre (local files)
    **kwargs
        Provider-specific keyword arguments forwarded to the source
        constructor (e.g. ``root=`` for ``"gesla"`` / ``"grdc"``,
        ``dataset_id=`` for ``"emodnet"`` / ``"copernicus"``).

    Returns
    -------
    StationSource
        Concrete station source instance for the requested provider.

    Raises
    ------
    ValueError
        If *name* does not match a known data source.
    """
    providers = {
        "ndbc": NDBCSource,
        "noaa_coops": NOAASource,
        "ioc": IOCSource,
        "usgs": USGSSource,
        "waterinfo": WaterinfoSource,
        "emodnet": EMODnetSource,
        "copernicus": CopernicusSource,
        "gesla": GESLASource,
        "grdc": GRDCSource,
    }
    try:
        return providers[name](**kwargs)
    except KeyError:
        raise ValueError(
            f"Unknown source: {name}. Choose from: {', '.join(providers)}"
        ) from None
