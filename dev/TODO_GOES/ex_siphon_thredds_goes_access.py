#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 18:02:39 2022

@author: ghiggi
"""
from urllib.request import urlopen
from siphon.catalog import TDSCatalog
from siphon.simplewebservice.ndbc import NDBC

#### Unidata THREDDS Data Server
# - GOES-R Data of last 14 days
# https://thredds-test.unidata.ucar.edu/thredds/catalog/satellite/goes16/GOES17/catalog.html

## Download GOES 16 data
base_cat_url = "http://thredds-test.unidata.ucar.edu/thredds/catalog/satellite/goes16/{platform}/{sector}/{channel}/current/catalog.xml"
urls = []

# Access ABI channels 1-3 to use in making a true color image (ABI has 16 total channels)
for channel in ["Channel{:02d}".format(x) for x in range(1, 4)]:
    cat_url = base_cat_url.format(platform="GOES16", sector="CONUS", channel=channel)
    cat = TDSCatalog(cat_url)
    # get the latest dataset from each
    ds = cat.datasets[-1]
    # get the opendap url for this dataset
    urls.append(ds.access_urls["OPENDAP"])


def get_goes_image(date=datetime.utcnow(), channel=8, region="CONUS"):
    """Return dataset of GOES-16 data."""
    cat = TDSCatalog(
        "https://thredds.ucar.edu/thredds/catalog/satellite/goes/east/products/"
        "CloudAndMoistureImagery/{}/Channel{:02d}/{:%Y%m%d}/"
        "catalog.xml".format(region, channel, date)
    )

    ds = cat.datasets[-1]  # Get most recent dataset
    ds = ds.remote_access(service="OPENDAP")
    ds = NetCDF4DataStore(ds)
    ds = xr.open_dataset(ds)
    return ds
