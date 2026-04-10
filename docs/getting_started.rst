Getting started
===============

Installation
------------

Install from GitHub:

.. code-block:: bash

   pip install git+https://github.com/Deltares-research/cht_observations.git

For development:

.. code-block:: bash

   git clone https://github.com/Deltares-research/cht_observations.git
   cd cht_observations
   pip install -e ".[tests]"

Quick example -- NOAA CO-OPS water level
-----------------------------------------

Fetch hourly water-level data from NOAA CO-OPS station 8638610
(Sewells Point, VA) for the first week of January 2024:

.. code-block:: python

   from datetime import datetime
   import cht_observations.observation_stations as obs

   # Create a NOAA CO-OPS source object
   coops = obs.source("noaa_coops")

   # Download water-level data
   df = coops.get_data(
       id=8638610,
       tstart=datetime(2024, 1, 1),
       tstop=datetime(2024, 1, 7),
       varname="water_level",
       datum="MSL",
       units="SI",
   )

   print(df.head())

Quick example -- NDBC buoy stations
-------------------------------------

List all active NDBC buoys and convert to a GeoDataFrame:

.. code-block:: python

   import cht_observations.observation_stations as obs

   ndbc = obs.source("ndbc")
   stations = ndbc.get_active_stations()
   gdf = ndbc.gdf()  # GeoDataFrame with point geometries

   print(f"Found {len(gdf)} active buoys")
