"""NOAA CO-OPS (Center for Operational Oceanographic Products and Services) station source."""

from datetime import datetime
from typing import Any

import pandas as pd
import requests
from noaa_coops import Station

from cht_observations._station_source import StationSource


class NOAASource(StationSource):
    """Station source backed by NOAA CO-OPS tide and current stations.

    Attributes
    ----------
    active_stations : list
        Cached list of active station dicts populated by ``get_active_stations``.
    """

    def __init__(self) -> None:
        self.active_stations = []

    def get_active_stations(self) -> list[dict[str, Any]]:
        """Fetch the list of active NOAA CO-OPS stations.

        Retrieves station metadata from the NOAA Tides and Currents API and
        stores the result in ``self.active_stations``.

        Returns
        -------
        list[dict[str, Any]]
            List of dicts with keys ``name``, ``id``, ``lon``, and ``lat``.
        """
        data_url = (
            "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json"
        )
        response = requests.get(data_url)
        json_dict = response.json()

        station_list = []
        for station_dict in json_dict["stations"]:
            station_list.append(
                {
                    "name": station_dict["name"],
                    "id": station_dict["id"],
                    "lon": float(station_dict["lng"]),
                    "lat": float(station_dict["lat"]),
                }
            )

        self.active_stations = station_list
        return station_list

    def get_meta_data(self, id: int) -> dict[str, Any]:
        """Retrieve metadata for a single NOAA CO-OPS station.

        Parameters
        ----------
        id : int
            NOAA CO-OPS station identifier.

        Returns
        -------
        dict[str, Any]
            Station metadata dict.
        """
        station = Station(id=str(id))
        meta_data = station.metadata
        return meta_data

    def get_data(
        self,
        id: int,
        tstart: datetime,
        tstop: datetime,
        varname: str = "water_level",
        units: str = "SI",
        datum: str = "MSL",
    ) -> pd.DataFrame:
        """Retrieve observational data from a NOAA CO-OPS station.

        Parameters
        ----------
        id : int
            NOAA CO-OPS station identifier.
        tstart : datetime
            Start time of the requested data window.
        tstop : datetime
            End time of the requested data window.
        varname : str, optional
            Variable name to retrieve (default ``"water_level"``).
        units : str, optional
            Unit system; ``"SI"`` is converted to ``"metric"`` for the API
            (default ``"SI"``).
        datum : str, optional
            Vertical datum, e.g. ``"MSL"`` or ``"NAVD"`` (default ``"MSL"``).

        Returns
        -------
        pd.DataFrame
            Single-column DataFrame containing the requested variable values
            indexed by time.
        """
        t0_string = tstart.strftime("%Y%m%d %H:%M")
        t1_string = tstop.strftime("%Y%m%d %H:%M")

        if varname == "water_level":
            product = varname
            product_output = "v"
        if units == "SI":
            units = "metric"

        station = Station(id=id)
        df = station.get_data(
            begin_date=t0_string,
            end_date=t1_string,
            product=product,
            datum=datum,
            units=units,
            time_zone="gmt",
        )
        return df[product_output].to_frame()
