"""GRDC (Global Runoff Data Centre) source.

GRDC distributes its data only after registering at
https://portal.grdc.bafg.de/. Once you've been granted access, export
the station catalog and per-station monthly/daily text files and point
this source at the local folder.

Expected layout::

    <root>/
        GRDC_stations.xlsx      # or .csv with lat/long/grdc_no columns
        data/
            <grdc_no>_Q_Day.Cmd.txt   # daily discharge, one per station
            ...

Per-station files use the GRDC "GRDC Export Format" — lines starting
with ``#`` (metadata) then a two-column ``YYYY-MM-DD;hh:mm;value`` body
(semicolon separated). Parsing is best-effort and version-tolerant.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from cht_observations._station_source import StationSource


class GRDCSource(StationSource):
    """GRDC daily-discharge reader backed by a local export.

    Parameters
    ----------
    root : str
        Folder containing the GRDC station catalog and per-station files.
    metadata_file : str, optional
        Station catalog filename (default ``"GRDC_stations.xlsx"``). A
        ``.csv`` is also accepted.
    """

    def __init__(
        self,
        root: str,
        metadata_file: str = "GRDC_stations.xlsx",
    ) -> None:
        self.active_stations = []
        self.root = Path(root)
        self.metadata_file = metadata_file

    def _load_catalog(self) -> pd.DataFrame:
        path = self.root / self.metadata_file
        if not path.exists():
            raise FileNotFoundError(f"GRDC catalog not found: {path}")
        if path.suffix.lower() in (".xlsx", ".xls"):
            return pd.read_excel(path)
        return pd.read_csv(path)

    def get_active_stations(self) -> list[dict[str, Any]]:
        """Parse the GRDC station catalog."""
        df = self._load_catalog()
        col_no = next(
            (c for c in df.columns if c.lower() in ("grdc_no", "grdc-no", "grdcno")),
            None,
        )
        col_name = next(
            (c for c in df.columns if c.lower() in ("station", "station name", "name")),
            None,
        )
        col_lat = next((c for c in df.columns if c.lower().startswith("lat")), None)
        col_lon = next(
            (
                c
                for c in df.columns
                if c.lower().startswith("long") or c.lower() == "lon"
            ),
            None,
        )
        if None in (col_no, col_name, col_lat, col_lon):
            raise RuntimeError(f"Unexpected GRDC catalog columns: {list(df.columns)}")

        station_list = []
        for _, row in df.iterrows():
            try:
                station_list.append(
                    {
                        "name": str(row[col_name]),
                        "id": str(int(row[col_no])),
                        "lon": float(row[col_lon]),
                        "lat": float(row[col_lat]),
                    }
                )
            except (ValueError, TypeError):
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
        tstart: Optional[datetime] = None,
        tstop: Optional[datetime] = None,
        suffix: str = "_Q_Day.Cmd.txt",
    ) -> pd.DataFrame:
        """Load a single GRDC station file.

        Parameters
        ----------
        id : str
            GRDC station number.
        tstart, tstop : datetime, optional
            Optional time window.
        suffix : str, optional
            Filename suffix to look up under ``<root>/data`` or ``<root>``.
            Default ``"_Q_Day.Cmd.txt"`` (daily discharge).
        """
        candidates = [
            self.root / "data" / f"{id}{suffix}",
            self.root / f"{id}{suffix}",
        ]
        path = next((p for p in candidates if p.exists()), None)
        if path is None:
            raise FileNotFoundError(
                f"GRDC data file not found for station {id}; tried: {candidates}"
            )

        df = pd.read_csv(
            path,
            sep=";",
            comment="#",
            engine="python",
            names=["date", "time", "discharge"],
            na_values=["-999.000", "-999", "-9999"],
            on_bad_lines="skip",
        )
        # Drop header rows that may have slipped through.
        df = df[pd.to_datetime(df["date"], errors="coerce").notna()]
        df["time"] = pd.to_datetime(
            df["date"].astype(str) + " " + df["time"].astype(str).fillna("00:00"),
            utc=True,
            errors="coerce",
        )
        df = df.dropna(subset=["time"]).set_index("time")
        if tstart is not None:
            df = df[df.index >= pd.Timestamp(tstart, tz="UTC")]
        if tstop is not None:
            df = df[df.index <= pd.Timestamp(tstop, tz="UTC")]
        return df[["discharge"]]
