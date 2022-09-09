#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 10:58:35 2022

@author: ghiggi
"""
import os
import dask
import fsspec

local_fpaths = [
    "/tmp/GOES-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211130282_e20193211139590_c20193211140047.nc",
    "/tmp/GOES-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211140282_e20193211149590_c20193211150043.nc",
    "/tmp/GOES-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211150282_e20193211159590_c20193211200047.nc",
    "/tmp/GOES-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C02_G16_s20193211130282_e20193211139590_c20193211140036.nc",
    "/tmp/GOES-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C02_G16_s20193211140282_e20193211149590_c20193211150036.nc",
]

bucket_s3_fpaths = [
    "s3://noaa-goes16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211130282_e20193211139590_c20193211140047.nc",
    "s3://noaa-goes16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211140282_e20193211149590_c20193211150043.nc",
    "s3://noaa-goes16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211150282_e20193211159590_c20193211200047.nc",
    "s3://noaa-goes16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C02_G16_s20193211130282_e20193211139590_c20193211140036.nc",
    "s3://noaa-goes16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C02_G16_s20193211140282_e20193211149590_c20193211150036.nc",
]

bucket_gcs_fpaths = [
    "gs://gcp-public-data-goes-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211130282_e20193211139590_c20193211140047.nc",
    "gs://gcp-public-data-goes-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211140282_e20193211149590_c20193211150043.nc",
    "gs://gcp-public-data-goes-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C01_G16_s20193211150282_e20193211159590_c20193211200047.nc",
    "gs://gcp-public-data-goes-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C02_G16_s20193211130282_e20193211139590_c20193211140036.nc",
    "gs://gcp-public-data-goes-16/ABI-L1b-RadF/2019/321/11/OR_ABI-L1b-RadF-M6C02_G16_s20193211140282_e20193211149590_c20193211150036.nc",
]


def create_local_directories(fpaths, exist_ok=True):
    _ = [os.makedirs(os.path.dirname(fpath), exist_ok=True) for fpath in fpaths]
    return None


@dask.delayed
def _download(bucket_fpath, local_fpath, protocol, fs_args):
    fs = fsspec.filesystem(protocol=protocol, **fs_args)
    fs.get(bucket_fpath, local_fpath)


create_local_directories(local_fpaths)

# --------------------------------------------------------------------------
### S3 Multiprocessing option [download once at time (or sometimes hang..)]
protocol = "s3"
fs_args = {"anon": True}

jobs = [
    _download(bucket_fpath, local_fpath, protocol, fs_args)
    # _download(bucket_fpath, local_fpath, protocol, fs_args)
    for local_fpath, bucket_fpath in zip(local_fpaths, bucket_s3_fpaths)
]

dask.compute(jobs, scheduler="processes")  # threads work ;)

# --------------------------------------------------------------------------
### GCS Multiprocessing option [download once at time, or hang]
protocol = "gcs"
fs_args = {"token": "anon"}

jobs = [
    _download(bucket_fpath, local_fpath, protocol, fs_args)
    # _download(bucket_fpath, local_fpath, protocol, fs_args)
    for local_fpath, bucket_fpath in zip(local_fpaths, bucket_gcs_fpaths)
]

dask.compute(jobs, scheduler="processes")

# --------------------------------------------------------------------------
### S3 Threading option [WORKS --> asynchronous parallel downloads]
protocol = "s3"
fs_args = {"anon": True}
jobs = [
    _download(bucket_fpath, local_fpath, protocol, fs_args)
    # _download(bucket_fpath, local_fpath, protocol, fs_args)
    for local_fpath, bucket_fpath in zip(local_fpaths, bucket_s3_fpaths)
]

dask.compute(jobs, scheduler="threads")

# -----------------------------------------------------------------------------.


# # Define download function
# @dask.delayed
# def _download(bucket_fpath, local_fpath):
#     fs.get(bucket_fpath, local_fpath)

# # Split list in n_threads blocks
# l_local_fpaths = _chunk_list(local_fpaths, n=n_threads)
# l_bucket_fpaths = _chunk_list(bucket_fpaths, n=n_threads)

# # Loop over each block and download in parallel
# # local_fpaths, bucket_fpaths = list(zip(l_local_fpaths, l_bucket_fpaths))[0]
# for local_fpaths, bucket_fpaths in zip(l_local_fpaths, l_bucket_fpaths):
#     # Define download jobs
#     jobs = [
#         _download(bucket_fpath, local_fpath)
#         for local_fpath, bucket_fpath in zip(local_fpaths, bucket_fpaths)
#     ]
#     # Try to download files
#     # TODO: ensure parallel download (async?)
#     # fs should be initiated within download function?
#     # - https://github.com/dask/dask/issues/7547

#     try:
#         dask.compute(jobs, scheduler="threads")
#     except FileNotFoundError:
#         print(
#             f" - Unable to download one of the following files: {bucket_fpaths}"
#         )
