#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 10:17:32 2020

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
from goes_api import download_files, find_files

dir(goes_api)

# date
# datetime / time
base_dir = None
protocol = "gcs"
protocol = "s3"
fs_args = {}

satellite = "GOES-16"
sensor = "ABI"

product_level = "L1B"
product = "Rad"  # this must be verified based on others
sector = "F"
# sector = "M" # M1 or M2 !!!
#  scene_abbr = ['M2'] # M1 or M2 for sector=="M"
scene_abbr = None
scan_modes = None
channels = None

# scene_abbr = None
filter_parameters = {}
filter_parameters["scan_modes"] = scan_mode
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

start_time = datetime.datetime(2019, 11, 17, 11, 30)
end_time = datetime.datetime(2019, 11, 17, 13, 50)

verbose = True
connection_type = None
connection_type = "nc_bytes"

# Download args
n_threads = 5
force_download = False
check_data_integrity = True
return_fpaths = True
verbose = True
filter_parameters = {}
base_dir = "/tmp/"

l_files = download_files(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    n_threads=n_threads,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
)

fpaths = find_files(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    connection_type=connection_type,
    group_by_key="start_time",
    verbose=True,
)
fpaths

fpath_dict = find_files(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    group_by_key="start_time",
    verbose=True,
)
fpath_dict
fpath_dict.keys()

fpath_dict = group_files(
    fpaths, sensor=sensor, product_level=product_level, key="start_time"
)

# TODO:
# - available_group_keys
# - group_files(key) # invalid try !!!


def _get_cpus():
    if max_cpus is None:
        max_cpus = multiprocessing.cpu_count()
    cpus = np.minimum(multiprocessing.cpu_count(), max_cpus)
    cpus = np.minimum(cpus, n)


#### Download options
# - Download specific hour folder
# - Download specific date (download per doy)
# - date --> download directories if no filtering asked

##  Methods
# - rclone  <--- for bulk download ? how to parallelize
#   https://github.com/blaylockbk/pyBKB_v3/blob/master/rclone_howto.md

# rclone copy publicAWS:noaa-goes16/ABI-L2-MCMIPC/2018/283/00/OR_ABI-L2-MCMIPC-M3_G16_s20182830057203_e20182830059576_c20182830100076.nc ./

# - http download (wget, ... )

##----------------------------------------------------------------------------.
## List online GOES-16 netCDF data
# get_available_online_product(protocol="s3", satellite="goes-16")

# get_available_online_product(protocol="gcs", satellite="goes-16")

# ------------------------------------------------------------------------------.
# Turn some attributes to coordinates so they will be preserved
# when we concat multiple GOES DataSets together.
attr2coord = [
    "dataset_name",
    "date_created",
    "time_coverage_start",
    "time_coverage_end",
]


def _attrs_2_coords(ds, attrs):
    for key in attrs:
        if key in ds.attrs:
            ds.coords[key] = ds.attrs.pop(key)
    return ds
