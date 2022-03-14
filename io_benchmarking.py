#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 10:12:55 2022

@author: ghiggi
"""
# Compare remapping between 
# - local
# - http iobytes
# - nc_byte
# - S3
# - kerchunk 
# - download, process, delete 

# - Chelle Gentemann: Any plan to kerchunk GOES L1B? 
import os
import datetime
import fsspec
import netCDF4
import xarray as xr
 
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
sector = "C"
# sector = "M" # M1 or M2 !!!
#  scene_abbr = ['M2'] # M1 or M2 for sector=="M"
scene_abbr = None
scan_modes = None
channels = ['C01', 'C02', 'C11']

# scene_abbr = None
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

start_time = datetime.datetime(2019, 11, 17, 11, 30)
end_time = datetime.datetime(2019, 11, 17, 11, 40)

verbose = True
connection_type = None
# connection_type = "nc_bytes"

# Download args
n_threads = 5
force_download = False
check_data_integrity = True
return_fpaths = True
verbose = True
# filter_parameters = {}
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
    protocol="local",   # BUGGY ... do not use protocol if base_dir provided ! Accept None !!! 
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
    group_by_key=None,
    verbose=True,
)
fpaths

ds = xr.open_dataset(l_files[4])
ds['Rad'].shape
ds['Rad'].nbytes/1024/1024 # MB 

# GOES 16 SHAPE, MEMORY SIZE, CHUNK SIZE, CHUNK MEMORY (float32),  CHUNK DISK (int16)
# - Full Disc CHUNK SIZE: 226x226  --> 0.19 MB per chunk (memory) --> 99 KB on disk
# - CONUS     CHUNK SIZE: 250x250  --> 0.23 MB per chunk (memory) --> 122 KB disk
# - Mesoscale CHUNK SIZE: 250x250  --> 0.23 MB per chunk (memory) --> 122 KB disk

# FD: (10848,10848)    449 MB     2304 CHUNKS
#     (21696, 21696)   1.75 GB    9216 CHUNKS
#     (5424, 5424)     112 MB     576 CHUNKS
    
# C: (3000, 5000)     57 MB       240 CHUNKS
#    (6000, 10000)    228 MB      960 CHUNKS
#    (1500, 2500)     14 MB       60 CHUNKS

ds['Rad'].isel(x=slice(0,226), y=slice(0,226)).nbytes/1024/1024
