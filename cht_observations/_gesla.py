"""GESLA (Global Extreme Sea Level Analysis) source.

GESLA is a historic archive of quality-controlled hourly tide-gauge
records. It is distributed as a bulk download — one data file per
station plus a single metadata CSV — rather than as a live API. Obtain
the archive from https://gesla.org and point this source at the local
folder.

Directory layout expected by this source::

    <root>/
        GESLA3_ALL.csv       # or any metadata csv with Lat/Long/File/Name
        data/                # folder with per-station data files

The per-station data files are plain text, two columns (date-time,
value) with a short header — the ``# Number of header lines:`` line
gives the header length. Parsing tries to be forgiving.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from cht_observations._station_source import StationSource


class GESLASource(StationSource):
    """Bulk GESLA archive reader.

    Parameters
    ----------
    root : str or Path
        Folder containing the GESLA metadata CSV and the per-station
        data files (typically ``<root>/data``).
    metadata_file : str, optional
        Metadata CSV filename (default ``"GESLA3_ALL.csv"``).
    """

    def __init__(
        self,
        root: str,
        metadata_file: str = "GESLA3_ALL.csv",
    ) -> None:
        self.active_stations = []
        self.root = Path(root)
        self.metadata_file = metadata_file
        self._meta: Optional[pd.DataFrame] = None

    def _load_meta(self) -> pd.DataFrame:
        if self._meta is None:
            path = self.root / self.metadata_file
            if not path.exists():
                raise FileNotFoundError(f"GESLA metadata not found: {path}")
            self._meta = pd.read_csv(path)
        return self._meta

    def get_active_stations(self) -> list[dict[str, Any]]:
        """Load the station list from the GESLA metadata CSV."""
        meta = self._load_meta()
        # Column names vary slightly between GESLA versions; be lenient.
        col_name = next(
            (
                c
                for c in meta.columns
                if c.lower().startswith("site name") or c.lower() == "name"
            ),
            None,
        )
        col_file = next(
            (
                c
                for c in meta.columns
                if c.lower().startswith("file") or c.lower() == "filename"
            ),
            None,
        )
        col_lat = next((c for c in meta.columns if c.lower().startswith("lat")), None)
        col_lon = next(
            (
                c
                for c in meta.columns
                if c.lower().startswith("lon") or c.lower().startswith("long")
            ),
            None,
        )

        if None in (col_name, col_file, col_lat, col_lon):
            raise RuntimeError(
                f"Unexpected GESLA metadata columns: {list(meta.columns)}"
            )

        station_list = []
        for _, row in meta.iterrows():
            station_list.append(
                {
                    "name": str(row[col_name]),
                    "id": str(row[col_file]),
                    "lon": float(row[col_lon]),
                    "lat": float(row[col_lat]),
                }
            )
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
    ) -> pd.DataFrame:
        """Load a single GESLA station file.

        The GESLA format begins with a ``# Number of header lines: N``
        line; the body is whitespace-separated ``date time value [flag]``.
        """
        data_dir = self.root / "data"
        path = data_dir / id if data_dir.exists() else self.root / id
        if not path.exists():
            raise FileNotFoundError(f"GESLA data file not found: {path}")

        # Find the header length by scanning the first ~50 lines.
        header_lines = 0
        with open(path, encoding="utf-8", errors="ignore") as f:
            for line in f:
                if line.startswith("#"):
                    header_lines += 1
                    if "Number of header lines" in line:
                        try:
                            header_lines = int(line.split(":")[-1].strip())
                        except ValueError:
                            pass
                else:
                    break

        df = pd.read_csv(
            path,
            sep=r"\s+",
            skiprows=header_lines,
            names=["date", "time", "water_level", "flag"],
            engine="python",
            usecols=[0, 1, 2, 3],
            on_bad_lines="skip",
        )
        df["time"] = pd.to_datetime(
            df["date"] + " " + df["time"], utc=True, errors="coerce"
        )
        df = df.dropna(subset=["time"]).set_index("time")
        if tstart is not None:
            df = df[df.index >= pd.Timestamp(tstart, tz="UTC")]
        if tstop is not None:
            df = df[df.index <= pd.Timestamp(tstop, tz="UTC")]
        return df[["water_level"]]
