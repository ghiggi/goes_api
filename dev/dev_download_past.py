#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 11:13:47 2022

@author: ghiggi
"""
import os
import s3fs
import numpy as np
import tqdm
import netCDF4
import xarray as xr
import matplotlib.pyplot as plt
import cartopy
import cartopy.crs as ccrs

import os
import datetime
import fsspec

from goes_api.listing import PRODUCTS
from goes_api.listing import GLOB_FNAME_PATTERN


import goes_api
from goes_api import find_closest_start_time, download_previous_files, download_next_files

dir(goes_api)

base_dir = None
protocol = "s3"
fs_args = {}
satellite = "GOES-16"
sensor = "ABI"
product_level = "L1B"
product = "Rad"  # this must be verified based on others
sector = "M"
scene_abbr = ['M2'] # M1 or M2 for sector=="M"
channels = ['C01']
scan_modes = None
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

time = datetime.datetime(2019, 11, 17, 11, 30)

verbose = True
connection_type = None

# Download args
n_threads = 5
force_download = False
check_data_integrity = True
N=5

include_start_time=False
operational_checks=True

return_fpaths = True
verbose = True
progress_bar = True
base_dir = "/tmp/"


time = find_closest_start_time(
        time = time,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        protocol="s3",
        filter_parameters=filter_parameters,
)

start_time=time
fpaths = download_previous_files(
    start_time=time,
    N=5,
    include_start_time=False,
    operational_checks=True,
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    n_threads=n_threads,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True
)
print(fpaths)
assert len(fpaths) == 5
    
fpaths = download_next_files(
    start_time=time,
    N=5,
    include_start_time=True,
    operational_checks=True,
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    n_threads=n_threads,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True
)

print(fpaths)
assert len(fpaths) == 5
    