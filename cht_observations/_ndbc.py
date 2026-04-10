"""NDBC (National Data Buoy Center) station source implementation."""

from typing import Any, Optional

import pandas as pd
from NDBC.NDBC import DataBuoy

from cht_observations import utils
from cht_observations._station_source import StationSource


class NDBCSource(StationSource):
    """Station source backed by the NOAA National Data Buoy Center (NDBC).

    Parameters
    ----------
    None

    Attributes
    ----------
    active_stations : list
        Cached list of active station dicts populated by ``get_active_stations``.
    db : DataBuoy
        Underlying NDBC DataBuoy client.
    """

    db: DataBuoy

    def __init__(self) -> None:
        self.active_stations = []
        self.db = DataBuoy()

    def get_active_stations(self) -> list[dict[str, Any]]:
        """Fetch the list of currently active NDBC stations.

        Retrieves station metadata from the NDBC active-stations XML feed and
        stores the result in ``self.active_stations``.

        Returns
        -------
        list[dict[str, Any]]
            List of dicts with keys ``name``, ``id``, ``lon``, and ``lat``.
        """
        url = "https://www.ndbc.noaa.gov/activestations.xml"
        obj = utils.xml2obj(url)
        station_list = []
        for station in obj.station:
            station_list.append(
                {
                    "name": station.name,
                    "id": station.id,
                    "lon": float(station.lon),
                    "lat": float(station.lat),
                }
            )
        self.active_stations = station_list
        return station_list

    def get_meta_data(self, id: int) -> Optional[dict[str, Any]]:
        """Retrieve metadata for a single NDBC station.

        Parameters
        ----------
        id : int
            NDBC station identifier.

        Returns
        -------
        dict[str, Any] or None
            Station metadata dict, or ``None`` if the request fails.
        """
        self.db.set_station_id(id)
        try:
            meta_data = self.db.station_info
        except Exception as e:
            meta_data = None
            print(e)
        return meta_data

    def get_data(self, id: int, variable: Optional[Any] = None) -> pd.DataFrame:
        """Retrieve observational data for a single NDBC station.

        Parameters
        ----------
        id : int
            NDBC station identifier.
        variable : Any, optional
            Variable to retrieve. Currently unused; reserved for future use.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the requested data.
        """
        pass
