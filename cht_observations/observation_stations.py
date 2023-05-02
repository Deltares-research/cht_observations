"""
Example:

import datetime
import cht.observation_stations.observation_stations as obs

coops = obs.source("noaa_coops")
t0 = datetime.datetime(2015, 1, 1)
t1 = datetime.datetime(2015, 1, 10)
df = coops.get_data("9447130", t0, t1)

"""

import datetime

class StationSource:
    def __init__(self):
        pass

    def list_stations(self):
        pass

    def get_meta_data(self):
        pass

    def get_data(self):
        pass

def source(name):
    if name == "ndbc":
        from _ndbc import Source
        return Source()
    elif name == "noaa_coops":
        from _noaa_coops import Source
        return Source()
