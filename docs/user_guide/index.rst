User guide
==========

The source pattern
------------------

All data providers are accessed through a single factory function:

.. code-block:: python

   import cht_observations.observation_stations as obs

   src = obs.source("noaa_coops")   # or "ndbc"

The returned object exposes three core methods:

``get_active_stations()``
    Returns a list of dicts with keys ``name``, ``id``, ``lon``, ``lat``.
    Also cached in ``src.active_stations``.

``get_meta_data(id)``
    Returns a metadata dict for a single station.

``get_data(...)``
    Downloads observational time series and returns a
    :class:`pandas.DataFrame`. The exact signature differs by provider.

``gdf()``
    Converts ``active_stations`` to a :class:`geopandas.GeoDataFrame`
    with EPSG:4326 point geometries.

NOAA CO-OPS
-----------

NOAA CO-OPS provides tide-gauge and coastal sensor data through the
`Tides and Currents API <https://tidesandcurrents.noaa.gov/>`_.

List active stations::

   import cht_observations.observation_stations as obs

   coops = obs.source("noaa_coops")
   stations = coops.get_active_stations()
   gdf = coops.gdf()

Fetch water-level data::

   from datetime import datetime

   df = coops.get_data(
       id=8638610,
       tstart=datetime(2024, 1, 1),
       tstop=datetime(2024, 1, 7),
       varname="water_level",
       datum="MSL",
       units="SI",
   )

The returned DataFrame has a time index and a column ``v`` containing
water-level values in metres.

NDBC
----

NDBC provides data from offshore buoys worldwide.

List active buoys::

   import cht_observations.observation_stations as obs

   ndbc = obs.source("ndbc")
   stations = ndbc.get_active_stations()
   gdf = ndbc.gdf()

Retrieve metadata for a single buoy::

   meta = ndbc.get_meta_data(id=44013)   # Boston, MA
