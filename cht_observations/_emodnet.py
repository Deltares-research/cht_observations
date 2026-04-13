"""EMODnet Physics source (European marine in-situ observations).

Thin wrapper around the EMODnet ERDDAP endpoint at
``https://erddap.emodnet-physics.eu/erddap``. No authentication needed.

EMODnet aggregates national monitoring programmes across Europe and
exposes them as per-platform tabledap datasets. The default ERDDAP
dataset is ``EP_ERD_INT_TZFX_AL_TS_NRT`` (near-real-time sea level). A
different dataset id can be passed to :py:meth:`get_data` / the
constructor for waves, currents, etc.
"""

from datetime import datetime
from io import StringIO
from typing import Any, Optional

import pandas as pd
import requests

from cht_observations._station_source import StationSource

BASE = "https://erddap.emodnet-physics.eu/erddap"


class EMODnetSource(StationSource):
    """EMODnet Physics platform source via ERDDAP."""

    def __init__(self, dataset_id: str = "EP_ERD_INT_TZFX_AL_TS_NRT") -> None:
        self.active_stations = []
        self.dataset_id = dataset_id

    def get_active_stations(
        self,
        bbox: Optional[tuple[float, float, float, float]] = None,
    ) -> list[dict[str, Any]]:
        """Fetch the platform inventory for the configured ERDDAP dataset.

        Parameters
        ----------
        bbox : tuple, optional
            ``(minLon, minLat, maxLon, maxLat)`` to limit the query.
        """
        url = f"{BASE}/tabledap/{self.dataset_id}.csv"
        params = {"platform_code,longitude,latitude": None}
        # ERDDAP wants query like: ?platform_code,longitude,latitude&distinct()
        # requests' params dict doesn't handle that nicely, so build manually.
        query = "platform_code,longitude,latitude&distinct()"
        if bbox is not None:
            query += (
                f"&longitude>={bbox[0]}&longitude<={bbox[2]}"
                f"&latitude>={bbox[1]}&latitude<={bbox[3]}"
            )
        response = requests.get(f"{url}?{query}")
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text), skiprows=[1])

        station_list = []
        for _, row in df.iterrows():
            try:
                code = str(row["platform_code"])
                station_list.append(
                    {
                        "name": code,
                        "id": code,
                        "lon": float(row["longitude"]),
                        "lat": float(row["latitude"]),
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
        variable: str = "SLEV",
    ) -> pd.DataFrame:
        """Fetch a time series for a single platform.

        Parameters
        ----------
        id : str
            EMODnet ``platform_code``.
        tstart, tstop : datetime
            Time window (UTC).
        variable : str, optional
            ERDDAP column name, default ``"SLEV"`` (sea level).
        """
        url = f"{BASE}/tabledap/{self.dataset_id}.csv"
        query = (
            f"time,{variable}"
            f"&platform_code=%22{id}%22"
            f"&time>={tstart.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            f"&time<={tstop.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        response = requests.get(f"{url}?{query}")
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text), skiprows=[1])
        df["time"] = pd.to_datetime(df["time"], utc=True)
        return df.set_index("time")[[variable]]
