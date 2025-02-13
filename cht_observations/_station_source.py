from abc import ABC, abstractmethod
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
import shapely


class StationSource(ABC):
    active_stations: list

    @abstractmethod
    def get_active_stations(self) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def get_meta_data(self, id: int) -> Optional[dict[str, Any]]:
        pass

    @abstractmethod
    def get_data(
        self,
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        pass

    def gdf(self) -> gpd.GeoDataFrame:
        gdf_list = []
        # Loop through points
        for station in self.active_stations:
            name = station["name"]
            x = station["lon"]
            y = station["lat"]
            id = station["id"]
            point = shapely.geometry.Point(x, y)
            d = {"id": id, "name": name, "geometry": point}
            gdf_list.append(d)
        return gpd.GeoDataFrame(gdf_list, crs=4326)
