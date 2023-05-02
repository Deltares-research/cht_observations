from cht_observations.observation_stations import StationSource
from NDBC.NDBC import DataBuoy


class Source(StationSource):
    def __init__(self):
        self.db = DataBuoy()

    def list_stations(self):
        pass

    def get_meta_data(self, id):
        self.db.set_station_id(id)
        return getattr(self.db, "station_info", None)

    def get_data(self, id, variable=None):
        pass
