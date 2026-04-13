"""USGS NWIS (National Water Information System) station source.

Covers US rivers, reservoirs and tidal stations. Uses the public
``waterservices.usgs.gov`` REST API; no authentication required.
"""

from datetime import datetime
from io import StringIO
from typing import Any, Optional

import pandas as pd
import requests

from cht_observations._station_source import StationSource

# Common USGS parameter codes
#   00060 — discharge (ft^3/s)
#   00065 — gage height (ft)
#   62614 — reservoir elevation (ft)
DEFAULT_PARAM = "00060"


class USGSSource(StationSource):
    """USGS NWIS water data (discharge, gage height, elevation)."""

    SITE_URL = "https://waterservices.usgs.gov/nwis/site/"
    IV_URL = "https://waterservices.usgs.gov/nwis/iv/"

    def __init__(self) -> None:
        self.active_stations = []

    def get_active_stations(
        self,
        bbox: Optional[tuple[float, float, float, float]] = None,
        state: Optional[str] = None,
        parameter: str = DEFAULT_PARAM,
    ) -> list[dict[str, Any]]:
        """Fetch active stations.

        NWIS requires a spatial or administrative filter; pass ``bbox``
        (minLon, minLat, maxLon, maxLat) or a two-letter ``state``.
        """
        params: dict[str, Any] = {
            "format": "rdb",
            "siteStatus": "active",
            "parameterCd": parameter,
            "siteType": "ST,LK,ES",  # stream, lake, estuary
        }
        if bbox is not None:
            params["bBox"] = ",".join(f"{v:.4f}" for v in bbox)
        elif state is not None:
            params["stateCd"] = state
        else:
            raise ValueError("USGS NWIS requires a `bbox` or `state` filter.")

        response = requests.get(self.SITE_URL, params=params)
        response.raise_for_status()
        # Strip USGS RDB comment lines (start with '#')
        lines = [ln for ln in response.text.splitlines() if not ln.startswith("#")]
        df = pd.read_csv(StringIO("\n".join(lines)), sep="\t", skiprows=[1])

        station_list = []
        for _, row in df.iterrows():
            try:
                station_list.append(
                    {
                        "name": row["station_nm"],
                        "id": str(row["site_no"]),
                        "lon": float(row["dec_long_va"]),
                        "lat": float(row["dec_lat_va"]),
                    }
                )
            except (KeyError, ValueError):
                continue
        self.active_stations = station_list
        return station_list

    def get_meta_data(self, id: str) -> Optional[dict[str, Any]]:
        for s in self.active_stations:
            if s["id"] == id:
                return s
        return None

    def get_data(
        self,
        id: str,
        tstart: datetime,
        tstop: datetime,
        parameter: str = DEFAULT_PARAM,
    ) -> pd.DataFrame:
        """Fetch an instantaneous-values time series for a station.

        Parameters
        ----------
        id : str
            USGS site number.
        tstart, tstop : datetime
            Time window (UTC).
        parameter : str, optional
            NWIS parameter code, default ``"00060"`` (discharge, ft^3/s).

        Returns
        -------
        pd.DataFrame
            Single-column DataFrame indexed by UTC time. Column name is the
            parameter code.
        """
        params = {
            "format": "json",
            "sites": id,
            "startDT": tstart.strftime("%Y-%m-%dT%H:%MZ"),
            "endDT": tstop.strftime("%Y-%m-%dT%H:%MZ"),
            "parameterCd": parameter,
        }
        response = requests.get(self.IV_URL, params=params)
        response.raise_for_status()
        data = response.json()
        ts_list = data["value"]["timeSeries"]
        if not ts_list:
            return pd.DataFrame(columns=[parameter])
        values = ts_list[0]["values"][0]["value"]
        df = pd.DataFrame(values)
        df["dateTime"] = pd.to_datetime(df["dateTime"], utc=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.set_index("dateTime").rename(columns={"value": parameter})
        return df[[parameter]]
