from typing import Any, Optional

import geopandas as gpd
import pandas as pd
import pytest

from cht_observations._ndbc import NDBCSource
from cht_observations._noaa_coops import NOAASource
from cht_observations._station_source import StationSource
from cht_observations.observation_stations import source


class MockStationSource(StationSource):
    def __init__(self):
        self.active_stations = [
            {"name": "Station 1", "id": "001", "lon": -70.0, "lat": 40.0},
            {"name": "Station 2", "id": "002", "lon": -71.0, "lat": 41.0},
        ]

    def get_active_stations(self) -> list[dict[str, Any]]:
        return self.active_stations

    def get_meta_data(self, id: int) -> Optional[dict[str, Any]]:
        return None

    def get_data(self, *args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame()


@pytest.fixture
def mock_station_source():
    return MockStationSource()


def mock_get_active_stations(self):
    self.active_stations = [
        {"name": "Station 1", "id": "001", "lon": -70.0, "lat": 40.0},
        {"name": "Station 2", "id": "002", "lon": -71.0, "lat": 41.0},
    ]
    return self.active_stations


def test_gdf(mock_station_source):
    gdf = mock_station_source.gdf()
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 2
    assert all(col in gdf.columns for col in ["id", "name", "geometry"])
    assert gdf.crs.to_epsg() == 4326


def test_ndbc_gdf(monkeypatch):
    monkeypatch.setattr(NDBCSource, "get_active_stations", mock_get_active_stations)
    ndbc_source = NDBCSource()
    ndbc_source.get_active_stations()
    gdf = ndbc_source.gdf()
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 2
    assert all(col in gdf.columns for col in ["id", "name", "geometry"])
    assert gdf.crs.to_epsg() == 4326


def test_noaa_coops_gdf(monkeypatch):
    monkeypatch.setattr(NOAASource, "get_active_stations", mock_get_active_stations)
    noaa_source = NOAASource()
    noaa_source.get_active_stations()
    gdf = noaa_source.gdf()
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 2
    assert all(col in gdf.columns for col in ["id", "name", "geometry"])
    assert gdf.crs.to_epsg() == 4326


@pytest.mark.parametrize(
    "source_name, source_class", [("ndbc", NDBCSource), ("noaa_coops", NOAASource)]
)
def test_source(source_name, source_class):
    s = source(source_name)
    assert isinstance(s, source_class)


def test_source_unknown_raises():
    with pytest.raises(ValueError):
        source("unknown")
