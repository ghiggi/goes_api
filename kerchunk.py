#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 18:54:28 2022

@author: ghiggi
"""


# -----------------------------------------------------------------------------.
##### KERCHUNK
# NetCDF4/HDF5 files designed for filesytem use
# - Suffer of performance issues when accessed from cloud object storage
# Zarr was specifically designed to overcome such issues
# --> Metadata are stored in a single JSON object
# --> Each chunk is a separate object
# --> Enable unlimited file sizes and parallel writes/reads
# --> Avoid HDF5/netCDF4 library issues with concurrent multi-threaded reads.

# HDF5/NetCDF files could be directly opened from s3 but performance is poor
#  because HDF5/NetCDF library executes many small reads and s3 storage has
#   significantly higher latency than traditional file systems

# Extracting the chunk byte ranges of NetCDF4/HDF5 into a JSON metadata file
# --> Extract internal references of netCDF4/HDF5 file
# --> Exteact chunk locations to enable the byte-range requests.

# A single metadata file can point to exactly where the required data are
# (instead having to access all files to understand which are necessary)


# Chunk query API feature introduced in the HDF5 1.10.5 library.

# JSON metadata standards

{
    "key0": "data",
    "key1": {
        [
            "protocol://target_url",
            10000,
            100,
        ]  # [data_file_url, offset_within_file (in bytes), length_data_item (in bytes)]
    },
}

# Performance
# - How much takes each request (Signell: 350ms)
# --> Dask profiler !

# May not perform well if the chunk sizes are too small.
# If chunk sizes are less than a few MB, the read times could drop to less than 100ms # https://distributed.dask.org/en/latest/limitations.html#limitations
# Chunk sizes in the range of 10–200MB work well with Dask reading from Zarr data in cloud storage
# Amazon’s Best Practices for S3 recommends making concurrent requests for byte ranges of an object at the granularity of 8–16 MB
# TODO: what is a chunk of GOES ABI ?

# if dask chunks>hdf chunks
# If a dask task fetches multiple underlying files using the zarr interface, and the backend supports it (s3, gcs, http(s), currently), then the fetches will be concurrent and latency can be reduced by a large factor in some cases.

# async will give a speed-up if multiple chunks are being read at once
#   (i.e., the dask partition is larger than the zarr chunksize by some factor),
#  and that the latency/overhead of each request is a significant fraction of the overall time.
#  Once you are in the regime that you were bandwidth limited anyway, as opposed to waiting,
#   then async doesn’t help.

# async should typically help when we encounter a dataset in object storage with lots of little chunks
#  but not so much when we read datasets with 100mb chunks like we recommend, r

# async lessens the overhead for smaller chunks (so long as they are loaded concurrently,
#   meaning a dask partition containing many chunks)

# One consequence of the async capability is that there is less of a performance penalty
#   for using smaller Zarr chunks. You still want to use Dask chunks of ~100MB for large compute jobs.
#   But the underlying Zarr chunks can be smaller.

# s3fs and gcsfs will now internally load the small chunks asynchronously inside each task.

# collections and grouped data ? How to access?

# Maximum request rate limit
# - Amazon S3: 6000/s.
# - GCP: 500/s
# - Microsoft Azure Blob Storage: 500/s

# TODO How much does it size a JSON? How much does it take to create?


# Kerchunk
# https://fsspec.github.io/kerchunk/cases.html

# GOES tutorial

# https://github.com/lsterzinger/fsspec-reference-maker-tutorial
# https://github.com/lsterzinger/fsspec-reference-maker-tutorial/blob/main/tutorial.ipynb !!!

# https://github.com/lsterzinger/cloud-optimized-satellite-data-tests

# https://lucassterzinger.com/assets/siparcs-2021/Sterzinger_Lucas_Slides.pdf
# https://medium.com/pangeo/fake-it-until-you-make-it-reading-goes-netcdf4-data-on-aws-s3-as-zarr-for-rapid-data-access-61e33f8fe685

# Another tutorial
# https://github.com/lsterzinger/2022-esip-kerchunk-tutorial
# - https://github.com/lsterzinger/2022-esip-kerchunk-tutorial/blob/main/01-Create_References.ipynb !!!

# Chelle Gentemann (SST-Tutorial)
# https://nbviewer.org/github/cgentemann/cloud_science/blob/master/zarr_meta/cloud_mur_v41_benchmark.ipynb

# Richard Signell (USGS)
# https://medium.com/pangeo/cloud-performant-reading-of-netcdf4-hdf5-data-using-the-zarr-library-1a95c5c92314
# https://medium.com/pangeo/cloud-performant-netcdf4-hdf5-with-zarr-fsspec-and-intake-3d3a3e7cb935

## Other snippets:
# - https://gist.github.com/rsignell-usgs/ef435a53ac530a2843ce7e1d59f96e22 # file-nwm_forecast_make_json_parallel-ipynb
# -  https://gist.github.com/rsignell-usgs/6c6585cb2b52c6385720009fd30c0519 # Efficient access to the NWM medium

# - https://gist.github.com/rsignell-usgs/b12d6b8c91fb3e933bd0b0c2868cb3fd # ERA5 !!!
# - https://gist.github.com/rsignell-usgs/413b2a31aec1641b10f1250937262e05
# - https://gist.github.com/rsignell-usgs/da74d35ce28979babe7338ca698108c5 # ERA5
# - https://gist.github.com/rsignell-usgs/378be399f3c065fc5b90e503faf3e64d # ERA5 local

# - https://gist.github.com/rsignell-usgs/6117f63ba1b266ea713249b98492ce76 # GRIDMET
# - https://gist.github.com/rsignell-usgs/40cd834d44bfa7c0e8621a8b81adb53a # GRIDMET

# - https://gist.github.com/rsignell-usgs?page=2

# Combine 3 kerchunked datasets
# - https://gist.github.com/rsignell-usgs/1ca99bbda72f70dccdf86976d7279a08

# MultiZarrToZarr: kwargs
# - https://github.com/lsterzinger/fsspec-help/blob/main/example.ipynb


# 1. Extract the metadata into an fsspec.referenceFileSystem file
# 2. Create a mapper
# 3. Read the mapper using the Zarr library.

import fsspec

from fsspec.registry import known_implementations

known_implementations.keys()

gcs = fsspec.filesystem("gcs")
gcs.ls("pangeo-data/SDO_AIA_Images/")

# Create referenceFileSystem
fs = fsspec.filesystem(
    "reference",
    fo="gcs://mdtemp/SDO_no_coords.json",
    remote_options={"token": "anon"},
    remote_protocol="gcs",
    target_options={"token": "anon"},
)

fs.ls("", False)
fs.ls("094", False)[:5]
print(fs.cat("094/.zarray").decode())
fs.references["094/0.0.0"]

fs.size("....nc") / 1e9
flist = fs.glob("s3://era5-pds/2019/*/data/air_temperature_at_2_metres.nc")
flist

ds = xr.open_dataset(fs.open(mon_files[0]))

fs = s3fs.S3FileSystem(anon=True)

flist = fs.glob("noaa-goes16/ABI-L2-SSTF/2021/138/*/*.nc")
f = [fs.open(file) for file in flist]

# From mapper ... read data
import fsspec
import xarray as xr
import fsspec

target_options = {"requester_pays": True}
ref_storage_args = {"requester_pays": True}

target_options = {"anon": True, "compression": "zstd"}
ref_storage_args = {}

target_protocol = "http"
target_protocol = "s3"

references = "s3://pangeo-data-uswest2/esip/adcirc/adcirc_01d_offsets.json"

mapper = fsspec.get_mapper(
    "reference://",
    references=references,
    ref_storage_args=ref_storage_args,
    target_protocol=target_protocol,
    target_options=target_options,
)

ds = xr.open_dataset(mapper, engine="zarr", backend_kwargs={"consolidated": False})

ds = xr.open_zarr(mapper)
ds.nbytes / 1e6  # MB
ds.nbytes / 1e9  # GB
ds.nbytes / 1e12  # TB

json_consolidated = "s3://esip-qhub-public/nasa/mur/murv41_consolidated_20211011.json"

s_opts = {"requester_pays": True, "skip_instance_cache": True}
r_opts = {
    "key": response["accessKeyId"],
    "secret": response["secretAccessKey"],
    "token": response["sessionToken"],
    "client_kwargs": {"region_name": "us-west-2"},
}

fs = fsspec.filesystem(
    "reference",
    fo=json_consolidated,
    ref_storage_args=s_opts,
    remote_protocol="s3",
    remote_options=r_opts,
    simple_templates=True,
)
m = fs.get_mapper("")
ds = xr.open_dataset(m, decode_times=False, engine="zarr", consolidated=False)

# Alternative:
json_file = "s3://...json"
remote_options = {
    "key": response["accessKeyId"],
    "secret": response["secretAccessKey"],
    "token": response["sessionToken"],
    "client_kwargs": {"region_name": "us-west-2"},
}
remote_options = {anon: True}
ref_storage_args = {"requester_pays": True, "skip_instance_cache": True}
ref_storage_args = {"skip_instance_cache": True}

fs = fsspec.filesystem(
    "reference",
    fo=json_file,
    remote_protocol="s3",
    ref_storage_args=ref_storage_args,
    remote_options=remote_options,
    simple_templates=True,
)
mapper = fs.get_mapper("")
ds = xr.open_dataset(mapper, engine="zarr")
ds = xr.open_dataset(m, decode_times=False, engine="zarr", consolidated=False)

# Optimize for uncompressed chunk size !


import fsspec.implementations.reference as refs
import intake_xarray
import intake
import zarr


import appdirs

cachedir = appdirs.user_cache_dir("ABI-block-cache")


# catalog of file content and dataset byte storage information.


# https://medium.com/pangeo/cloud-performant-reading-of-netcdf4-hdf5-data-using-the-zarr-library-1a95c5c92314
# Modified Zarr and xarray:  chunk_store to xr.open_zarr ,  FileChunkStore  to zarr
# chunkstore contains the chunk locations to enable the byte-range requests.
# Option 1
ncfile = ffspec.open("s3...", anon=False, requester_pays=True)

store = ffspec.get_mapper("s3... .nc.chunkstore", anom=True)
chunk_store = FileChunkStore(store, chunk_source=ncfile.open())

ds = xr.open_zarr(store, consolidated=True, chunk_store=chunk_store)


ds = xr.open_dataset(ncfile.open(), engine="h5netcdf", decode_times=False)

# Direct access via fs
files = fs.glob(
    join("podaac-ops-cumulus-protected/", "MUR-JPL-L4-GLOB-v4.1", "2005*.nc")
)
ds = xr.open_dataset(file.open())
