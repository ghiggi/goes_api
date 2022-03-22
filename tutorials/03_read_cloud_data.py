#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 11:13:47 2022

@author: ghiggi
"""
import datetime
import xarray as xr
from goes_api import find_latest_files

# NOTE: For GOES performance benchmarks, check https://github.com/ghiggi/goes_benchmarks/
###---------------------------------------------------------------------------.
#### Define protocol  
base_dir = None

protocol = "gcs"
protocol = "s3"
fs_args = {}

###---------------------------------------------------------------------------.
#### Define satellite, sensor, product_level and product 
satellite = "GOES-16"
sensor = "ABI"
product_level = "L1B"
product = "Rad"  

###---------------------------------------------------------------------------.
#### Define sector and filtering options 
sector = "M"
scene_abbr = ['M1'] # None download and find both locations
scan_modes = None  # select all scan modes (M3, M4, M6)
channels = None    # select all channels
channels = ['C01'] # select channels subset
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

####---------------------------------------------------------------------------.
#### Open file using in-memory buffering via https and bytesIO  
fpaths = find_latest_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    connection_type="https"
)
# - Select http url 
fpath = list(fpaths.values())[0][0]
print(fpath)

# - Open via bytesIO
import requests
from io import BytesIO
resp = requests.get(fpath)
f_obj = BytesIO(resp.content)
ds = xr.open_dataset(f_obj)
ds['Rad'].plot.imshow()

####---------------------------------------------------------------------------.
#### Open file using netCDF byte range requests
# - This is quite slow !!!
fpaths = find_latest_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    connection_type="nc_bytes"
)
# - Retrieve http url with #mode=bytes suffix
fpath = list(fpaths.values())[0][0]
print(fpath)

# - Open via netcdf #mode=bytes 
ds = xr.open_dataset(fpath, engine="netcdf4")
ds['Rad'].plot.imshow()  

####---------------------------------------------------------------------------.
#### Open file using netCDF byte range requests
# - This is quite slow !!!
fpaths = find_latest_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    connection_type="bucket"
)
# - Retrieve bucket url 
fpath = list(fpaths.values())[0][0]
print(fpath)

# - Open via fsspec
import fsspec
fs = fsspec.filesystem('s3', anon=True)
ds = xr.open_dataset(fs.open(fpath), engine='h5netcdf')
ds['Rad'].plot.imshow()  

####---------------------------------------------------------------------------.