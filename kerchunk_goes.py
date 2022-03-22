#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 11:27:01 2022

@author: ghiggi
"""
import os
import glob
import fsspec
import ujson
import kerchunk 
import numpy as np 
from kerchunk.hdf import SingleHdf5ToZarr 
from goes_api.io import infer_satellite_from_path, remove_bucket_address
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

import datetime

import dask
from dask.distributed import Client
client = Client(n_workers=20)
client


import goes_api 
from goes_api import find_files


json_dir = "/ltenas3/0_Data/kerchunk_json/"
protocol = "gcs"
protocol = "s3"
fs_args = {}

satellite = "GOES-16"
sensor = "ABI"

product_level = "L1B"
product = "Rad"   
sector = "F"
# sector = "M" # M1 or M2 !!!
# scene_abbr = ['M2'] # M1 or M2 for sector=="M"
scene_abbr = None
scan_modes = None
channels = None # ['C01', 'C02', 'C15']

# scene_abbr = None
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

start_time = datetime.datetime(2020, 07, 05, 11, 30)
end_time = datetime.datetime(2020, 07, 08, 11, 50)

fpaths = find_files(
    base_dir=None,
    protocol="s3",   
    fs_args={},
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    connection_type=None,
    group_by_key=None,
    verbose=True,
)
 


## Create Reference JSON 

# json_dir = "/tmp/json_goes16"

# fs_args = {
#     "mode": "rb",
#     "anon": True,
#     "default_fill_cache": False, 
#     "default_cache_type":"none",
# }

fpath = "s3://noaa-goes16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C14_G16_s20193211130282_e20193211139590_c20193211140079.nc'"
url = fpath 
 
 
from goes_api.kerchunk import generate_kerchunk_files 
protocol = "s3"
generate_kerchunk_files(
    json_dir=json_dir,
    n_processes=20, 
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
    verbose=True,
    progress_bar=True, 
)
 
# 4 hours per day of data
# 
#        
            
# Apply in parallel to many files  
out = [gen_json(fpath,
                json_dir=json_dir, 
                fs_args=fs_args) for fpath in fpaths]

dask.compute(out)

# Time delayed or bucket timing ... 
# Dask bag 
# - Do not pass a delayed function 
import dask.bag as db
n_partitions = min(len(flist), 500)  
bag = db.from_sequence(list_inputs, npartitions=n_partitions).map(func)
bag.visualize()
dicts = bag.compute()


# Replace bucket url 
d['templates']['u'] = # ... d['templates']['u']


# List JSON files 
json_pattern = "/tmp/json_goes16/GOES16/ABI-L1b-RadF/2019/321/11/*.json"
json_list = sorted(glob.glob(json_pattern))

# Create Mapper objects 
m_list = []
for j in tqdm(json_list):
    with open(j) as f:
        m_list.append(fsspec.get_mapper("reference://", 
                        fo=ujson.load(f),
                        remote_protocol='s3',
                        remote_options={'anon':True}))


fs = fsspec.filesystem("reference", fo="./combined.json",
                       remote_protocol="s3", 
                        remote_options={"anon":True},
                        skip_instance_cache=True)
m = fs.get_mapper("")
ds = xr.open_dataset(m, engine='zarr')

##----------------------------------------------------------------------------.

 
# IO Bytes
# - Download the entire object into memory and then create a file image and read that 
# - Avoid download to disk ... 


# f_obj = fsspec.open(s3_fpath, mode="r", anon=True) # r or rb ?
# f_obj.__fspath__() 

h5 = h5py.File(f_obj, mode="r")
h5nc = h5netcdf.File(f_obj, mode='r')
ds = xr.open_dataset(f_obj, engine="h5netcdf")
ds['Rad'].plot.imshow()
t_f = time.time() 
print(t_f - t_i)  



   # or
   nc4_ds = netCDF4.Dataset(self.file_name, memory=resp.content)
   fobj = xr.backends.NetCDF4DataStore(nc4_ds)