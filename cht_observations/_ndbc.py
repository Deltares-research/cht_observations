from typing import Any, Optional

import pandas as pd
from NDBC.NDBC import DataBuoy

from cht_observations import utils
from cht_observations._station_source import StationSource


class NDBCSource(StationSource):
    db: DataBuoy

    def __init__(self):
        self.active_stations = []
        self.db = DataBuoy()

    def get_active_stations(self) -> list[dict[str, Any]]:
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
        self.db.set_station_id(id)
        try:
            meta_data = self.db.station_info
        except Exception as e:
            meta_data = None
            print(e)
        return meta_data

    def get_data(self, id: int, variable: Optional[Any] = None) -> pd.DataFrame:
        pass
