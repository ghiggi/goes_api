#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 14 14:48:53 2020

@author: ghiggi
"""

from urllib.request import urlopen
from siphon.catalog import TDSCatalog
from siphon.simplewebservice.ndbc import NDBC

#### Unidata THREDDS Data Server  
# - GOES-R Data of last 14 days 

# https://www.unidata.ucar.edu/software/mcidas/adde_servers.html (lot of link)
goeseast.unidata.ucar.edu
goeswest.unidata.ucar.edu
satepsanone.nesdis.noaa.gov

# https://thredds-test.unidata.ucar.edu/thredds/catalog/satellite/goes16/GOES17/catalog.html
## Download GOES 16 data 
base_cat_url = 'http://thredds-test.unidata.ucar.edu/thredds/catalog/satellite/goes16/{platform}/{sector}/{channel}/current/catalog.xml'
urls = []
# Access ABI channels 1-3 to use in making a true color image (ABI has 16 total channels)
for channel in ['Channel{:02d}'.format(x) for x in range(1, 4)]:
    cat_url = base_cat_url.format(platform='GOES16', sector='CONUS', channel=channel)
    cat = TDSCatalog(cat_url)
    # get the latest dataset from each
    ds = cat.datasets[-1]
    # get the opendap url for this dataset
    urls.append(ds.access_urls['OPENDAP'])    

def get_goes_image(date=datetime.utcnow(), channel=8, region='CONUS'):
    """Return dataset of GOES-16 data."""
    cat = TDSCatalog('https://thredds.ucar.edu/thredds/catalog/satellite/goes/east/products/'
                     'CloudAndMoistureImagery/{}/Channel{:02d}/{:%Y%m%d}/'
                     'catalog.xml'.format(region, channel, date))

    ds = cat.datasets[-1]  # Get most recent dataset
    ds = ds.remote_access(service='OPENDAP')
    ds = NetCDF4DataStore(ds)
    ds = xr.open_dataset(ds)
    return ds


##----------------------------------------------------------------------------.
#### NCEI Thredds - GridSat (Global 3-hourly IR from 1980 to 2021) 
# - 8 km resolution at equator 
# - Equal area grid is 0.07 degrees latitude 
# - From -70 to 70 N
https://www.ncei.noaa.gov/thredds/catalog/cdr/gridsat/catalog.html

##----------------------------------------------------------------------------.
#### PERSIANN-CDR
https://www.ncei.noaa.gov/thredds/catalog/cdr/persiann/catalog.html

##----------------------------------------------------------------------------.
#### Other NCEII data
https://www.ncei.noaa.gov/data/

##----------------------------------------------------------------------------.
#### Retrieve data via Siphon - AVHRR
# Load other data using MetPy and Siphon
import metpy.calc as mpcalc
from metpy.plots import StationPlot, add_metpy_logo, add_unidata_logo, add_timestamp
from metpy.units import units
from siphon.catalog import TDSCatalog
from siphon.simplewebservice.ndbc import NDBC
sst_cat = TDSCatalog('https://www.ncei.noaa.gov/thredds/catalog/OisstBase/NetCDF/AVHRR/201809/catalog.xml')
sst = sst_cat.datasets[-1].remote_access(use_xarray=True)
sst_data = sst.metpy.parse_cf('sst')

def get_zip(url):
    data = urlopen(url).read()
    return fiona.BytesCollection(data)

https://github.com/Unidata/siphon