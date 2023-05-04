from cht_observations.observation_stations import source
from datetime import datetime


def test_noaa_download():
    station_id = 8665530
    tstart = datetime(2023, 1, 1)
    tstop = datetime(2023, 1, 2)

    noaa_source = source("noaa_coops")
    df = noaa_source.get_data(station_id, tstart, tstop)

    assert df.dtype == "float64"
    assert df.index[0] == tstart
    assert df.index[-1] == tstop


def test_ndbc_download():
    station_id = "46042"
    ndbc_source = source("ndbc")
    ndbc_source.get_meta_data(station_id)

    assert ndbc_source.db.station_id == station_id
