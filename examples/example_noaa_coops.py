"""
Example: NOAA COOPS
"""

import datetime
import cht_observations.observation_stations as obs

coops = obs.source("noaa_coops")
t0 = datetime.datetime(2015, 1, 1)
t1 = datetime.datetime(2015, 1, 10)
df = coops.get_data("9447130", t0, t1)
