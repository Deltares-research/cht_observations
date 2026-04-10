cht_observations
################

``cht_observations`` provides a uniform Python interface for retrieving
observational data from ocean station networks. It currently supports:

* **NOAA NDBC** -- National Data Buoy Center (offshore buoys)
* **NOAA CO-OPS** -- Center for Operational Oceanographic Products and Services
  (tide gauges and coastal sensors)

The main entry point is the :func:`~cht_observations.observation_stations.source`
factory, which returns a source object for the requested provider.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   getting_started
   user_guide/index
   api/index
   changelog
