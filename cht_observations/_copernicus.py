"""Copernicus Marine in-situ source (authenticated).

Requires a Copernicus Marine account and the ``copernicusmarine`` Python
package::

    pip install copernicusmarine
    copernicusmarine login   # one-time, stores credentials on disk

Once logged in the client can subset any in-situ dataset. The defaults
target global near-real-time sea level (dataset id
``cmems_obs-ins_glo_phy-ssh_nrt_stations_irr``). Pass a different
``dataset_id`` to the constructor for waves, currents, physical
variables, etc.
"""

from datetime import datetime
from typing import Any, Optional

import pandas as pd

from cht_observations._station_source import StationSource


class CopernicusSource(StationSource):
    """Copernicus Marine in-situ (CMEMS) observations."""

    def __init__(
        self,
        dataset_id: str = "cmems_obs-ins_glo_phy-ssh_nrt_stations_irr",
    ) -> None:
        self.active_stations = []
        self.dataset_id = dataset_id
        try:
            import copernicusmarine  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "The Copernicus Marine source requires the `copernicusmarine` "
                "package. Install with `pip install copernicusmarine` and "
                "authenticate once via `copernicusmarine login`."
            ) from e

    def get_active_stations(
        self,
        bbox: Optional[tuple[float, float, float, float]] = None,
    ) -> list[dict[str, Any]]:
        """Open the dataset and return the platform list.

        Parameters
        ----------
        bbox : tuple, optional
            ``(minLon, minLat, maxLon, maxLat)``. Without a bbox the
            dataset can be very large; subsetting first is recommended.
        """
        import copernicusmarine

        kwargs: dict[str, Any] = {"dataset_id": self.dataset_id}
        if bbox is not None:
            kwargs.update(
                minimum_longitude=bbox[0],
                maximum_longitude=bbox[2],
                minimum_latitude=bbox[1],
                maximum_latitude=bbox[3],
            )
        ds = copernicusmarine.open_dataset(**kwargs)

        # Station index varies per dataset; try common names.
        platform_var = next(
            (v for v in ("platform_code", "station", "WMO") if v in ds),
            None,
        )
        station_list = []
        lon = ds["longitude"].values.ravel()
        lat = ds["latitude"].values.ravel()
        codes = ds[platform_var].values.ravel() if platform_var else range(len(lon))
        for c, x, y in zip(codes, lon, lat):
            code = c.decode() if isinstance(c, bytes) else str(c)
            station_list.append(
                {"name": code, "id": code, "lon": float(x), "lat": float(y)}
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
        tstart: datetime,
        tstop: datetime,
        variable: str = "SLEV",
    ) -> pd.DataFrame:
        """Fetch a time series for a single platform.

        Notes
        -----
        The Copernicus Marine API is dataset-specific. This implementation
        covers the common case where the dataset is indexed by a
        ``platform_code`` string and exposes a single variable of
        interest. Adapt as needed for multi-parameter datasets.
        """
        import copernicusmarine

        ds = copernicusmarine.open_dataset(
            dataset_id=self.dataset_id,
            start_datetime=tstart,
            end_datetime=tstop,
        )
        platform_var = next(
            (v for v in ("platform_code", "station", "WMO") if v in ds),
            None,
        )
        if platform_var is None:
            raise RuntimeError(
                "Cannot locate platform index in dataset; specify a different dataset_id."
            )
        mask = ds[platform_var].astype(str) == id
        sel = ds.where(mask, drop=True)
        df = sel[[variable]].to_dataframe().reset_index()
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df = df.set_index("time")
        return df[[variable]]
