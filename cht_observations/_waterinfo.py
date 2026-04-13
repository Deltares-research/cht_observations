"""Rijkswaterstaat Waterinfo / DDL (Dutch water observation data).

Uses the public DDL REST API at
``https://waterwebservices.rijkswaterstaat.nl/``. No authentication.

Defaults to water level (parameter ``WATHTE``) but any Aquo parameter
code can be requested via ``get_data``.
"""

from datetime import datetime
from typing import Any, Optional

import pandas as pd
import requests

from cht_observations._station_source import StationSource

BASE = "https://waterwebservices.rijkswaterstaat.nl"
CATALOG_URL = f"{BASE}/METADATASERVICES_DBO/OphalenCatalogus/"
OBS_URL = f"{BASE}/ONLINEWAARNEMINGENSERVICES_DBO/OphalenWaarnemingen"


class WaterinfoSource(StationSource):
    """Rijkswaterstaat DDL observation data."""

    def __init__(self) -> None:
        self.active_stations = []
        self._catalog: Optional[dict[str, Any]] = None

    def _fetch_catalog(self) -> dict[str, Any]:
        if self._catalog is None:
            response = requests.post(
                CATALOG_URL,
                json={
                    "CatalogusFilter": {
                        "Grootheden": True,
                        "Parameters": True,
                        "Compartimenten": True,
                        "Eenheden": True,
                        "Hoedanigheden": True,
                    }
                },
            )
            response.raise_for_status()
            self._catalog = response.json()
        return self._catalog

    def get_active_stations(self) -> list[dict[str, Any]]:
        """Fetch the Rijkswaterstaat location catalog (~700 stations)."""
        catalog = self._fetch_catalog()
        station_list = []
        for loc in catalog.get("LocatieLijst", []):
            try:
                station_list.append(
                    {
                        "name": loc["Naam"],
                        "id": loc["Code"],
                        "lon": float(loc["X"]),
                        "lat": float(loc["Y"]),
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
        grootheid: str = "WATHTE",
        hoedanigheid: str = "NAP",
    ) -> pd.DataFrame:
        """Fetch a time series from Waterinfo.

        Parameters
        ----------
        id : str
            DDL location code (e.g. ``"HOEKVHLD"``).
        tstart, tstop : datetime
            Time window (UTC).
        grootheid : str, optional
            Aquo quantity code, default ``"WATHTE"`` (water height).
        hoedanigheid : str, optional
            Quality code, default ``"NAP"`` (Dutch vertical datum).

        Returns
        -------
        pd.DataFrame
            Single-column DataFrame ``value`` indexed by UTC time.
        """
        # Resolve the location entry (needs the X, Y, coordinate system)
        catalog = self._fetch_catalog()
        loc = next(
            (l for l in catalog.get("LocatieLijst", []) if l["Code"] == id), None
        )
        if loc is None:
            raise ValueError(f"Unknown Rijkswaterstaat location code: {id}")

        payload = {
            "Locatie": {
                "X": loc["X"],
                "Y": loc["Y"],
                "Code": loc["Code"],
            },
            "AquoPlusWaarnemingMetadata": {
                "AquoMetadata": {
                    "Grootheid": {"Code": grootheid},
                    "Hoedanigheid": {"Code": hoedanigheid},
                }
            },
            "Period": {
                "Begindatumtijd": tstart.strftime("%Y-%m-%dT%H:%M:%S.000+00:00"),
                "Einddatumtijd": tstop.strftime("%Y-%m-%dT%H:%M:%S.000+00:00"),
            },
        }
        response = requests.post(OBS_URL, json=payload)
        response.raise_for_status()
        result = response.json()

        rows = []
        for reeks in result.get("WaarnemingenLijst", []):
            for m in reeks.get("MetingenLijst", []):
                rows.append(
                    {
                        "time": m["Tijdstip"],
                        "value": m["Meetwaarde"]["Waarde_Numeriek"],
                    }
                )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["time"] = pd.to_datetime(df["time"], utc=True)
        return df.set_index("time")[["value"]]
