from datetime import datetime

import pandas as pd

from cht_observations.observation_stations import source


def test_get_active_stations():
    noaa_source = source("noaa_coops")
    stations = noaa_source.get_active_stations()
    assert isinstance(stations, list)
    assert len(stations) > 0
    assert all(isinstance(station, dict) for station in stations)
    assert all("name" in station for station in stations)
    assert all("id" in station for station in stations)
    assert all("lon" in station for station in stations)
    assert all("lat" in station for station in stations)


def test_get_meta_data():
    noaa_source = source("noaa_coops")
    station_id = 8665530
    meta_data = noaa_source.get_meta_data(station_id)
    assert isinstance(meta_data, dict)
    assert "id" in meta_data
    assert meta_data["id"] == str(station_id)


def test_get_data():
    noaa_source = source("noaa_coops")
    station_id = 8665530
    tstart = datetime(2023, 1, 1)
    tstop = datetime(2023, 1, 2)
    varname = "water_level"
    units = "SI"
    datum = "MSL"

    df = noaa_source.get_data(station_id, tstart, tstop, varname, units, datum)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.index[0] == tstart
    assert df.index[-1] == tstop
    assert df.columns == ["v"]
    assert df.dtypes["v"] == "float64"
