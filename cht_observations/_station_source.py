"""Abstract base class defining the interface for observation station data sources."""

from abc import ABC, abstractmethod
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
import shapely


class StationSource(ABC):
    """Abstract base class for observation station data sources.

    Concrete subclasses must implement ``get_active_stations``,
    ``get_meta_data``, and ``get_data``.

    Attributes
    ----------
    active_stations : list
        List of station dicts populated by ``get_active_stations``.
    """

    active_stations: list

    @abstractmethod
    def get_active_stations(self) -> list[dict[str, Any]]:
        """Fetch all currently active stations from the data source.

        Returns
        -------
        list[dict[str, Any]]
            List of dicts with at minimum keys ``name``, ``id``, ``lon``,
            and ``lat``.
        """
        pass

    @abstractmethod
    def get_meta_data(self, id: int) -> Optional[dict[str, Any]]:
        """Retrieve metadata for a single station.

        Parameters
        ----------
        id : int
            Station identifier.

        Returns
        -------
        dict[str, Any] or None
            Metadata dict for the station, or ``None`` if unavailable.
        """
        pass

    @abstractmethod
    def get_data(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Retrieve observational data for a single station.

        Parameters
        ----------
        *args : Any
            Positional arguments defined by the concrete subclass.
        **kwargs : Any
            Keyword arguments defined by the concrete subclass.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the requested data.
        """
        pass

    def gdf(self) -> gpd.GeoDataFrame:
        """Convert ``active_stations`` to a GeoDataFrame.

        Returns
        -------
        gpd.GeoDataFrame
            GeoDataFrame with columns ``id``, ``name``, and ``geometry``
            (point geometries in EPSG:4326).
        """
        gdf_list = []
        for station in self.active_stations:
            name = station["name"]
            x = station["lon"]
            y = station["lat"]
            id = station["id"]
            point = shapely.geometry.Point(x, y)
            d = {"id": id, "name": name, "geometry": point}
            gdf_list.append(d)
        return gpd.GeoDataFrame(gdf_list, crs=4326)
