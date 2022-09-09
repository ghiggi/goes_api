#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 12:07:05 2022

@author: ghiggi
"""
 
from goes_api.io import infer_satellite_from_path, remove_bucket_address
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm


fpath = "s3://noaa-goes16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C14_G16_s20193211130282_e20193211139590_c20193211140079.nc'"
url = fpath 

            
# Apply in parallel to many files  
out = [gen_json(fpath,
                reference_dir=reference_dir, 
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



## Parallel (multiprocess) reading of zarr
import dask.bag as db 

reference_list = glob.glob("")
b = db.from_sequence(reference, npartitions=36)
zarr_list = b.map(open_zarr_fun)
with dask.config.set(scheduler='processes'):
    with ProgressBar(): 
        zarr = zarrs.compute() 
        
ds = xr.concat(zarrs, dim="time", coords="minimal", compat="override", combine_attrs='override')        
# xarray backend_kwargs 