#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  4 13:10:03 2022

@author: ghiggi
"""
import xarray as xr
import fsspec 
from fsspec.implementations.cached import CachingFileSystem

# Define filepaths and some settings 
gcs_fpath = "gs://gcp-public-data-goes-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211130282_e20193211139590_c20193211140047.nc"
s3_fpath = "s3://noaa-goes16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211130282_e20193211139590_c20193211140047.nc"

s3_storage_options = {"anon": True}
gcs_storage_options = {"token": 'anon'}
chunks_dict = {"Rad": (226, 226)}

# Define block cache 
fs_gcs = fsspec.filesystem(protocol="gcs", **gcs_storage_options)
fs_gcs_block = CachingFileSystem(fs=fs_gcs)

fs_s3 = fsspec.filesystem(protocol="s3", **s3_storage_options)
fs_s3_block = CachingFileSystem(fs=fs_s3)

# Open with GCS fails 
with fs_gcs_block.open(gcs_fpath) as f:
    ds = xr.open_dataset(f, engine="h5netcdf", chunks=chunks_dict)

# Open with S3 works
with fs_s3_block.open(s3_fpath) as f:
    ds = xr.open_dataset(f, engine="h5netcdf", chunks=chunks_dict)
    
     