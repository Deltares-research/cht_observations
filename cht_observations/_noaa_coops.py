from cht_observations.observation_stations import StationSource
from noaa_coops import Station

import requests


class Source(StationSource):
    def __init__(self):
        pass

    def get_active_stations(self):
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

    def get_meta_data(self, id):
        station = Station(id=id)
        meta_data = station.metadata
        return meta_data

    def get_data(
        self,
        id: int,
        tstart,
        tstop,
        varname: str = "water_level",
        units: str = "SI",
        datum: str = "MSL",
    ):
        """
        Get data from NOAA COOPS

        Parameters
        ----------
        id : str
            Station ID
        tstart : datetime
            Start time
        tstop : datetime
            Stop time
        varname : str
            Variable name
        units : str
            Units
        datum : str
            Datum, e.g. MSL or NAVD

        Returns
        -------
        df : pandas.DataFrame
            Data frame with data
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
        return df[product_output]
