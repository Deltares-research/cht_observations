"""
Example:

import datetime
import cht_observations.observation_stations as obs

coops = obs.source("noaa_coops")
t0 = datetime.datetime(2015, 1, 1)
t1 = datetime.datetime(2015, 1, 10)
df = coops.get_data("9447130", t0, t1)

"""


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
        from cht_observations._ndbc import Source

        return Source()
    elif name == "noaa_coops":
        from cht_observations._noaa_coops import Source

        return Source()
