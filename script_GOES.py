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
from goes_api.io import *
from goes_api.io import dt_to_year_doy_hour

import goes_api


dir(goes_api)
 

# date
# datetime / time 
base_dir = None 
protocol = "gcs"
protocol = "s3"
fs_args = {}

satellite = "GOES-16"
sensor= "ABI"

product_level = "L1B"
product = "Rad"  # this must be verified based on others 
sector = "F"
# sector = "M" # M1 or M2 !!!
#  scene_abbr = ['M2'] # M1 or M2 for sector=="M"
scene_abbr=None
scan_mode=None
channels=None

# scene_abbr = None
filter_parameters = {} 
filter_parameters['scan_mode'] = scan_mode
filter_parameters['channels'] = channels
filter_parameters['scene_abbr'] = scene_abbr

start_time = datetime.datetime(2019,11,17,11,30)
end_time = datetime.datetime(2019,11,17,13,50)

verbose = True 
connection_type = None 
connection_type = "nc_bytes"

fpaths = find_files(base_dir=base_dir,
                        protocol=protocol,
                        fs_args = fs_args,
                        satellite = satellite, 
                        sensor = sensor, 
                        product_level = product_level, 
                        product = product,
                        sector = sector,  
                        start_time = start_time,
                        end_time = end_time, 
                        filter_parameters = filter_parameters, 
                        connection_type = connection_type, 
                        group_by_key = "start_time",
                        verbose = True, 
                        )
fpaths

fpath_dict = find_files(base_dir=base_dir,
                        protocol=protocol,
                        fs_args = fs_args,
                        satellite = satellite, 
                        sensor = sensor, 
                        product_level = product_level, 
                        product = product,
                        sector = sector,  
                        start_time = start_time,
                        end_time = end_time, 
                        filter_parameters = filter_parameters, 
                        group_by_key = "start_time",
                        verbose = True, 
                        )
fpath_dict
fpath_dict.keys()
    
    


def _get_cpus():
    if max_cpus is None:
        max_cpus = multiprocessing.cpu_count()
    cpus = np.minimum(multiprocessing.cpu_count(), max_cpus)
    cpus = np.minimum(cpus, n)
            
#### Download options 
# - Over a timerange (start_time, end_time)
# - Download specific datetime (close to it)
# - Download specific hour folder
# - Download specific date (download per doy)
# - overwrite / force 
# - start_time, end_time 
# - datetime --> look over file 

# - date --> download directories if no filtering asked 


##  Methods 
# - fs.get  <--- for filtered download
#   fs.get('noaa-himawari8/AHI-L1b-FLDK/2020/09/04/2350/*', dst)
fs.get(src, str(dst))
# --> Jussi (dask, processing), goes2go (multiprocessing.Pool)

# fs outside the download function (loop)


# - rclone  <--- for bulk download ? 
#   https://github.com/blaylockbk/pyBKB_v3/blob/master/rclone_howto.md
# - http download (wget, ... ) 

# How to parallelize fs.get
 
# Jussi solution  
# fs = filesytem 
# def filter :
# def dask_delayed_download (with fs.get within) :
# loop over all hours ... 
#   for loop within one hour files 
#      dask_delayed_download
#   dask.compute(jobs, scheduler='processes')

# satpy tutorial 
def _download_gcs_files(globbed_files, fs, base_dir, force):
    filenames = []
    for fn in globbed_files:
        ondisk_fn = os.path.basename(fn)
        ondisk_pathname = os.path.join(base_dir, ondisk_fn)
        filenames.append(ondisk_pathname)

        if force and os.path.isfile(ondisk_pathname):
            os.remove(ondisk_pathname)
        elif os.path.isfile(ondisk_pathname):
            LOG.info("Found existing: {}".format(ondisk_pathname))
            continue
        LOG.info("Downloading: {}".format(ondisk_pathname))
        fs.get('gs://' + fn, ondisk_pathname)
    return filenames 

os.makedirs(subdir, exist_ok=True)


# How to parallelize rclone
 
# Applications 
# - Download previous N image from timestep X 
# - Download N images around timestep X

if start_year != end_year: 
#     Warning("You are searching for data over multiple years. " 
#             "This might take some time !!!")

# if start_year == end_year: 
#     if start_doy == end_doy: 
#         prefix_year = start_year 
#         prefix_doy = start_toy 
#         if start_hour == end_hour: 
#             prefix_hour == start_hour 




##----------------------------------------------------------------------------.
## List online GOES-16 netCDF data
get_available_online_product(protocol="s3", satellite="goes-16")

get_available_online_product(protocol="gcs", satellite="goes-16")

#----------------------------------------------------------------------------.

 
####----------------------------------------------------------------------------.
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


