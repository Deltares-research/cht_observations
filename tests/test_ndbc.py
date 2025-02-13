import pytest

from cht_observations import utils
from cht_observations._ndbc import Source


class MockDataBuoy:
    def __init__(self):
        self.station_id = None
        self.station_info = {
            "id": "46042",
            "name": "Test Station",
            "lon": -70.0,
            "lat": 40.0,
        }

    def set_station_id(self, station_id):
        self.station_id = station_id


@pytest.fixture
def mock_data_buoy(monkeypatch):
    def mock_init(self):
        self.active_stations = []
        self.db = MockDataBuoy()

    monkeypatch.setattr(Source, "__init__", mock_init)


@pytest.fixture
def ndbc_source(mock_data_buoy):
    return Source()


def test_get_active_stations(ndbc_source, monkeypatch):
    def mock_xml2obj(url):
        class Station:
            def __init__(self, name, id, lon, lat):
                self.name = name
                self.id = id
                self.lon = lon
                self.lat = lat

        class Obj:
            station = [
                Station("Station 1", "001", -70.0, 40.0),
                Station("Station 2", "002", -71.0, 41.0),
            ]

        return Obj()

    monkeypatch.setattr(utils, "xml2obj", mock_xml2obj)

    stations = ndbc_source.get_active_stations()
    assert isinstance(stations, list)
    assert len(stations) > 0
    assert all(isinstance(station, dict) for station in stations)
    assert all("name" in station for station in stations)
    assert all("id" in station for station in stations)
    assert all("lon" in station for station in stations)
    assert all("lat" in station for station in stations)


def test_get_meta_data(ndbc_source):
    station_id = "46042"
    meta_data = ndbc_source.get_meta_data(station_id)
    assert isinstance(meta_data, dict)
    assert "id" in meta_data
    assert meta_data["id"] == station_id
