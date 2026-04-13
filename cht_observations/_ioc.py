"""IOC Sea Level Station Monitoring Facility (ioc-sealevelmonitoring.org) source."""

from datetime import datetime
from io import StringIO
from typing import Any, Optional

import pandas as pd
import requests

from cht_observations._station_source import StationSource


class IOCSource(StationSource):
    """Global real-time tide gauge network maintained by the IOC / UNESCO.

    Data feed is served by ``www.ioc-sealevelmonitoring.org``; no
    authentication required. Station codes are short strings such as
    ``"vaki"`` (Vaki, Iceland).
    """

    BASE_URL = "http://www.ioc-sealevelmonitoring.org/service.php"

    def __init__(self) -> None:
        self.active_stations = []

    def get_active_stations(self) -> list[dict[str, Any]]:
        """Fetch the full station list (global)."""
        response = requests.get(
            self.BASE_URL, params={"query": "stationlist", "showall": "all"}
        )
        response.raise_for_status()
        data = response.json()

        station_list = []
        for s in data:
            try:
                station_list.append(
                    {
                        "name": s.get("Location") or s.get("location") or s.get("Code"),
                        "id": s.get("Code") or s.get("code"),
                        "lon": float(s.get("Lon", s.get("lon"))),
                        "lat": float(s.get("Lat", s.get("lat"))),
                    }
                )
            except (TypeError, ValueError):
                continue
        self.active_stations = station_list
        return station_list

    def get_meta_data(self, id: str) -> Optional[dict[str, Any]]:
        """Return the station entry from the cached station list."""
        for s in self.active_stations:
            if s["id"] == id:
                return s
        return None

    def get_data(
        self,
        id: str,
        tstart: datetime,
        tstop: datetime,
        sensor: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch water level time series for a station.

        Parameters
        ----------
        id : str
            IOC station code.
        tstart, tstop : datetime
            Time window (UTC).
        sensor : str, optional
            Sensor code (e.g. ``"prs"``, ``"rad"``). If ``None`` the
            service returns the default sensor.

        Returns
        -------
        pd.DataFrame
            Single-column DataFrame ``water_level`` indexed by UTC time.
        """
        params = {
            "query": "data",
            "code": id,
            "timestart": tstart.strftime("%Y-%m-%dT%H:%M:%S"),
            "timestop": tstop.strftime("%Y-%m-%dT%H:%M:%S"),
            "format": "tsv",
        }
        if sensor:
            params["sensor"] = sensor
        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text), sep="\t")
        time_col = next(
            (
                c
                for c in df.columns
                if c.lower().startswith("time") or c.lower() == "date"
            ),
            df.columns[0],
        )
        value_col = next((c for c in df.columns if c != time_col), df.columns[-1])
        df[time_col] = pd.to_datetime(df[time_col], utc=True)
        df = df.set_index(time_col).rename(columns={value_col: "water_level"})
        return df[["water_level"]]
