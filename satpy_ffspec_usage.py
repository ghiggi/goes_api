#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 15:21:12 2022

@author: ghiggi
"""

## tar file: just concat of data in the files (no compression)
## zipped: compress all file usually 
# ffspec.TarFileSystem
# ffspec.ZipFileSystem
# --> backends support reading from streams ???

# ERDAP SERVERS: https://github.com/IrishMarineInstitute/awesome-erddap
# https://unidata.github.io/siphon/latest/ 

##----------------------------------------------------------------------------.
# Direct reading from cloud storage is not currently available in Satpy
# Users can search on remote filesystems by passing an instance of an implementation of
#  `fsspec.spec.AbstractFileSystem` --> fs
# Searching is **NOT** recursive

# base_dir: base directory (can be an s3:// address)
# filter_parameters: Filename pattern metadata to filter on

# libnetcdf for HTTP BYTE RANGE REQUESTS
# conda install -c conda-forge "libnetcdf=4.7.4=*_105
# https://twitter.com/dopplershift/status/1286415993347047425

## ffspec 
# - Used to perform serial requests to retrieve metadata
# - Now perform concurrent requests -> async and faster
# simplecache::<https> 
# gcs://<<....> , storage_option in backend_kwargs 

# Satpy issues: 
# - https://github.com/pytroll/pytroll-examples/issues/35
    

####----------------------------------------------------------------------------.
#### SATPY WITH FSFILE
from satpy import Scene
from satpy.readers import FSFile
import fsspec

filename = "noaa-goes16/ABI-L1b-RadC/2019/001/17/*_G16_s20190011702186*"
the_files = fsspec.open_files("simplecache::s3://" + filename, s3={"anon": True})
type(the_files)  # fsspec.core.OpenFiles
type(the_files[0])  # fsspec.core.OpenFile
full_name
the_files[0].full_name

list_satpy_fs_files = [FSFile(open_file) for open_file in the_files]
scn = Scene(filenames=list_satpy_fs_files, reader="abi_l1b")
scn.available_dataset_names()
scn.available_composite_names()
scn.load(["true_color_raw"])
scn.show("true_color_raw")

####----------------------------------------------------------------------------.
#### FIND-FILES-AND-READERS
import datetime
import s3fs
import satpy.readers

fs = s3fs.S3FileSystem(anon=True)

dict_reader_fpaths = satpy.readers.find_files_and_readers(
    base_dir="s3://noaa-goes16/ABI-L1b-RadF/2019/321/*/",
    fs=fs,
    reader="abi_l1b",
    start_time=datetime.datetime(2019, 11, 17, 14, 40),
)
dict_reader_fpaths["abi_l1b"]
dict_reader_fpaths["abi_l1b"][0]

# --------------------------------------------------------------------------.
### Reading options
# 1. Find bucket fpath
# 2. Define filesystem with appropriate cache
# 3. Pass to FSFile(fpath, fs)
# 4. Launch satpy

import xarray as xr

fpath = "https://noaa-goes16.s3.amazonaws.com/ABI-L1b-RadM/2019/321/13/OR_ABI-L1b-RadM2-M6C05_G16_s20193211350027_e20193211350084_c20193211350138.nc#mode=bytes"
ds = xr.open_dataset(fpath)
ds

ds["Rad"].plot.imshow()

##----------------------------------------------------------------------------.
import s3fs
from pathlib import Path

#### glob for multiple file patterns with fsspec
fs = s3fs.S3FileSystem(anon=True)
dataPath = str(Path().home()) + "/DATA/ABI_AWS"
_ = fs.get("noaa-goes16/ABI-L1b-RadM/2021/359/12/*_G16_s2021359122[0-9]*", dataPath)

files = fs.ls("noaa-goes16/ABI-L1b-RadC/2019/240/00/")
with fs.open(files[0], "rb") as f:
    ds = xr.open_dataset(BytesIO(f.read()), engine="h5netcdf")

##----------------------------------------------------------------------------.
#### Open with ffspec
import fsspec, xarray
import fsspec, h5netcdf

s3_path = "s3://noaa-goes16/ABI-L1b-RadF/2019/321/14/OR_ABI-L1b-RadF-M6C01_G16_s20193211440283_e20193211449591_c20193211450047.nc"
with fsspec.open(s3_path, mode="rb", anon=True) as f:
    ds = xarray.open_dataset(f)
    print(ds["Rad"][400, 300])

with fsspec.open(s3_path, mode="rb", anon=True) as f:
    ds = h5netcdf.File(f)
    print(ds["Rad"][400, 300])


### fsspec.open_files + satpy.FSFile

# simplecache
# simplecache::<s3_url>  stores the data locally once it is downloaded
# - without simplecache:: it won't use any local storage

# Don't use s3={anon:True} but rather anon=True
# --> It's an oddity of fsspec and caches
from satpy import Scene
import fsspec

filename = "noaa-goes16/ABI-L1b-RadC/2019/001/17/*_G16_s20190011702186*"
the_files = fsspec.open_files(
    "simplecache::s3://" + filename, s3={"anon": True}
)  # THIS BUGS
the_files = fsspec.open_files(
    "simplecache::s3://" + filename, anon=True
)  # # s3={'anon': True})
from satpy.readers import FSFile

fs_files = [FSFile(open_file) for open_file in the_files]
scn = Scene(filenames=fs_files, reader="abi_l1b")
scn.load(["true_color_raw"])

#----------------------------------------------------------
import s3fs
import glob
import fsspec
from satpy.readers import FSFile

filename = 'noaa-goes16/ABI-L1b-RadC/2019/001/17/*_G16_s20190011702186*'

the_files = fsspec.open_files("simplecache::s3://" + filename, s3={'anon': True})

fs_files = [FSFile(open_file) for open_file in the_files]

scn = Scene(filenames=fs_files, reader='abi_l1b')

scn.load(['true_color_raw'])
lscn = scn.resample('moll')
lscn.show('true_color_raw')

## netCDF byte range requests
url = "https://noaa-goes16.s3.amazonaws.com/ABI-L1b-RadC/2019/001/00/OR_ABI-L1b-RadC-M3C14_G16_s20190010002187_e20190010004560_c20190010005009.nc#mode=bytes"
scn = Scene(reader='abi_l1b', filenames=[url])


# blockcache
# --> instead of simplecache
# --> makes the process a little faster, but still downloads a lot more than
#     when using remote reading without caching


# Others FSFile PR
# - https://github.com/pytroll/satpy/pull/1472 (TODO: Should be closed?)

# pass the file system with the file path object
# Satpy didn't work with pathlib objects

# satpy-scene
# Providing a file object instead of a string as file location which is currently equivalent to filename.

# ffspec/s3 support for find_files_and_readers

## Accessing data
# - via filename
# - via bytesIO

# - via ffspec
# - via kerchunk
# - via HTTP byte-ranges request from netcdf API ("#mode=bytes" to the end of URL)

# Perform byte range requests instead of downloading the whole file
# Custom binary formats are typically memory mapped (np.memmap) and require the file to be on a local disk
# -- HRIT, AHI HSD ...


# Intake support to Satpy
# - Intake catalog (perhaps accessing S3 storage) and give it to a Satpy Scene object and it would "just work".

# OpenDAP: Unidata's THREDDS access via siphon package

# Read from remote file storage
# https://github.com/pytroll/satpy/issues/1062

#### nethogs -v 3 to monitor data transfer

# cloud accesss
# https://stackoverflow.com/questions/37703634/how-to-import-a-text-file-on-aws-s3-into-pandas-without-writing-to-disk/51777553#51777553


# requests
# - module to load the file into memory (or better just open the stream)

# io.BytesIO object
# - accepted by xr.open_dataset

import requests
from io import BytesIO

url = self.filename
if "googleapis" in url or "amazonaws" in url or "core.windows" in url:
    resp = requests.get(url)
    f_obj = BytesIO(resp.content)
    # or
    nc4_ds = netCDF4.Dataset(self.file_name, memory=resp.content)
    fobj = xr.backends.NetCDF4DataStore(nc4_ds)

# satpy !!!
# --> check open_file_or_filename
f_obj = open_file_or_filename(self.filename)  # try doing self.filename.open()
# --> self.filename need to be a class with method.open() or it is assumed to be a string !

### unzip_file in satpy.readers
# if 'amazonaws' in filename:
#     resp = requests.get(filename)
#     bz2file = bz2.BZ2File(BytesIO(resp.content))
# else:
#     bz2file = bz2.BZ2File(filename) # original


# from contextlib import suppress
# def __del__(self):
#     """Close the NetCDF file that may still be open."""
#     with suppress(IOError, OSError, AttributeError):
#         self.nc.close()

#### HIMAWARI HSD: read binary data inot numpy array (is not netcdf in background)
# GOES-16 and 17 and FCI only data available via netCDF ...


##### FFSPEC
# https://www.anaconda.com/blog/fsspec-remote-caching

# -----------------------------------------------------------------------------.
#### Intake catalog
# https://github.com/fsspec/kerchunk/blob/main/examples/intake_catalog.yml

# -----------------------------------------------------------------------------.
#### CachingFileSystem vs. WholeFileCacheSystem
# https://github.com/pytroll/satpy/pull/1321
# --->  need to optimize block_size


import s3fs, fsspec.implementations.cached, xarray

cache_storage_block = "/data/gholl/cache/fscache/blockcache"
cache_storage_whole = "/data/gholl/cache/fscache/filecache"

fs_s3 = s3fs.S3FileSystem(anon=True)
fs_whole = fsspec.implementations.cached.WholeFileCacheFileSystem(
    fs=fs_s3,
    cache_storage=cache_storage_whole,
    check_files=False,
    expiry_times=False,
    same_names=True,
)
fs_block = fsspec.implementations.cached.CachingFileSystem(
    fs=fs_s3,
    cache_storage=cache_storage_block,
    check_files=False,
    expiry_times=False,
    same_names=True,
)
target = "noaa-goes16/ABI-L1b-RadF/2019/321/14/OR_ABI-L1b-RadF-M6C01_G16_s20193211440283_e20193211449591_c20193211450047.nc"
print("loading", target)
# with fs_s3.open(target) as of:
# with fs_block.open(target, block_size=2**20) as of:
with fs_whole.open(target) as of:
    with xarray.open_dataset(of, decode_cf=False, engine="h5netcdf") as ds:
        print(ds["Rad"][3000, 4000])
del ds, of  # prevents https://github.com/intake/filesystem_spec/issues/404


def get_fsfiles(start_date, end_date, sector="F", chans=14):
    """Return FSFile objects for GOES ABI for period.
    Sector can be "C", "F", "M1", or "M2".
    Chans is a channel number or an array of channel numbers.
    Returns a list of FSFile instances.
    """
    # from https://github.com/gerritholl/sattools/blob/master/src/sattools/abi.py
    cachedir = appdirs.user_cache_dir("ABI-block-cache")

    if not isinstance(chans, collections.abc.Iterable):
        chans = {chans}

    fs_s3 = s3fs.S3FileSystem(anon=True)

    fs_block = fsspec.implementations.cached.CachingFileSystem(
        fs=fs_s3,
        cache_storage=cachedir,
        cache_check=600,
        check_files=False,
        expiry_times=False,
        same_names=False,
    )

    # satpy can't search recursively, only directly in the same directory
    # therefore use typhon, and filter channels manually later
    abi_fileset = FileSet(
        path=f"noaa-goes16/ABI-L1b-Rad{sector[0]:s}/"
        "{year}/{doy}/{hour}/"
        f"OR_ABI-L1b-Rad{sector:s}-M6C*_G16_"
        "s{year}{doy}{hour}{minute}{second}*_e{end_year}{end_doy}"
        "{end_hour}{end_minute}{end_second}*_c*.nc",
        name="abi",
        fs=fs_s3,
    )
    files = list(
        fi
        for fi in abi_fileset.find(start_date, end_date)
        if any(f"C{c:>02d}_" in fi.path for c in chans)
    )
    fsfiles = [satpy.readers.FSFile(fi.path, fs=fs_block) for fi in files]
    return fsfiles


satpy.FSFile(path, fs_block)


# netCDF4 via HTTP bytes-range requests
bucket = "noaa-goes16"
path = "/ABI-L1b-RadF/2019/321/14/OR_ABI-L1b-RadF-M6C01_G16_s20193211440283_e20193211449591_c20193211450047.nc"
url = f"https://{bucket:s}.s3.amazonaws.com" + path + "#mode=bytes"
with netCDF4.Dataset(url, mode="r") as ds:
    print(ds["Rad"][3000, 4000])


# Perhaps the overhead of each request on this particular S3 bucket is too much
#  so even though NetCDF4 (the C library) is downloading as little as possible,
#  it takes so long to make each request it needs that it is much slower than
#  the other options

# the number of open files per process (1024 on my system) is quickly exhausted:
# --> ulimit can solve?

# Problem in closing xarray data ...

# https://github.com/pytroll/satpy/pull/1349
url = "https://noaa-goes16.s3.amazonaws.com/ABI-L1b-RadC/2019/001/00/OR_ABI-L1b-RadC-M3C14_G16_s20190010002187_e20190010004560_c20190010005009.nc#mode=bytes"
scn = Scene(reader="abi_l1b", filenames=[url])
scn.load(["C14"])
