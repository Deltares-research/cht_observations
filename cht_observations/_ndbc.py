from cht.observation_stations.observation_stations import StationSource
from NDBC.NDBC import DataBuoy

class Source(StationSource):
    def __init__(self):
        self.db = DataBuoy()

    def list_stations(self):
        pass

    def get_meta_data(self, id):
        self.db.set_station_id(id)
        try:
            meta_data = self.db.station_info
        except:
            meta_data = None
        return meta_data

    def get_data(self, id, variable=None):
        pass
