#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 15:21:12 2022

@author: ghiggi
"""

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

####----------------------------------------------------------------------------.
#### SATPY WITH FSFILE 
from satpy import Scene
from satpy.readers import FSFile
import fsspec
filename = 'noaa-goes16/ABI-L1b-RadC/2019/001/17/*_G16_s20190011702186*'
the_files = fsspec.open_files("simplecache::s3://" + filename, s3={'anon': True})
type(the_files)     # fsspec.core.OpenFiles
type(the_files[0])  # fsspec.core.OpenFile
full_name
the_files[0].full_name

list_satpy_fs_files = [FSFile(open_file) for open_file in the_files]
scn = Scene(filenames=list_satpy_fs_files, reader='abi_l1b')
scn.available_dataset_names()
scn.available_composite_names()
scn.load(['true_color_raw'])
scn.show('true_color_raw')    

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
                        start_time=datetime.datetime(2019, 11, 17, 14, 40))
dict_reader_fpaths['abi_l1b']
dict_reader_fpaths['abi_l1b'][0]

##---------------------------------------------------------------------------
 

#--------------------------------------------------------------------------.
### Reading options 
# 1. Find bucket fpath
# 2. Define filesystem with appropriate cache 
# 3. Pass to FSFile(fpath, fs) 
# 4. Launch satpy 

import xarray as xr 
fpath = "https://noaa-goes16.s3.amazonaws.com/ABI-L1b-RadM/2019/321/13/OR_ABI-L1b-RadM2-M6C05_G16_s20193211350027_e20193211350084_c20193211350138.nc#mode=bytes"
ds = xr.open_dataset(fpath)
ds

ds['Rad'].plot.imshow()
#--------------------------------------------------------------------------.

##----------------------------------------------------------------------------.

import s3fs
from pathlib import Path

#### glob for multiple file patterns with fsspec 
fs = s3fs.S3FileSystem(anon=True)
dataPath = str(Path().home())+'/DATA/ABI_AWS'
_ = fs.get('noaa-goes16/ABI-L1b-RadM/2021/359/12/*_G16_s2021359122[0-9]*', dataPath)

#### 
# find_files_and_readers can do globbing 

### wild cards expressions 


files = fs.ls('noaa-goes16/ABI-L1b-RadC/2019/240/00/')
with fs.open(files[0], 'rb') as f:
    ds = xr.open_dataset(BytesIO(f.read()), engine='h5netcdf')
    
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
filename = 'noaa-goes16/ABI-L1b-RadC/2019/001/17/*_G16_s20190011702186*'
the_files = fsspec.open_files("simplecache::s3://" + filename, s3={'anon': True}) # THIS BUGS 
the_files = fsspec.open_files("simplecache::s3://" + filename, anon=True) # # s3={'anon': True})
from satpy.readers import FSFile
fs_files = [FSFile(open_file) for open_file in the_files]
scn = Scene(filenames=fs_files, reader='abi_l1b')
scn.load(['true_color_raw'])

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

# JBRAVO EXAMPLE 
https://jhbravo.gitlab.io/geostationary-images/

# requests 
# - module to load the file into memory (or better just open the stream) 

# io.BytesIO object  
# - accepted by xr.open_dataset 

import requests
from io import BytesIO

url = self.filename 
if 'googleapis' in url or 'amazonaws' in url or 'core.windows' in url:
   resp = requests.get(url)
   f_obj = BytesIO(resp.content)
   # or  
   nc4_ds = netCDF4.Dataset(self.file_name, memory = resp.content)
   fobj = xr.backends.NetCDF4DataStore(nc4_ds)

# satpy !!! 
# --> check open_file_or_filename
f_obj = open_file_or_filename(self.filename) # try doing self.filename.open()
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

#-----------------------------------------------------------------------------.
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
    ["protocol://target_url", 10000, 100] # [data_file_url, offset_within_file (in bytes), length_data_item (in bytes)]
  }
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
#-  https://gist.github.com/rsignell-usgs/6c6585cb2b52c6385720009fd30c0519 # Efficient access to the NWM medium 

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
    target_options={"token": "anon"}
)

fs.ls("", False)
fs.ls("094", False)[:5]
print(fs.cat("094/.zarray").decode())
fs.references["094/0.0.0"]

fs.size('....nc')/1e9
flist = fs.glob('s3://era5-pds/2019/*/data/air_temperature_at_2_metres.nc')
flist

ds = xr.open_dataset(fs.open(mon_files[0]), 

fs = s3fs.S3FileSystem(anon=True)
 
flist = fs.glob("noaa-goes16/ABI-L2-SSTF/2021/138/*/*.nc")
f = [fs.open(file) for file in flist]

# From mapper ... read data 
import fsspec
import xarray as xr
import fsspec

target_options={'requester_pays': True}
ref_storage_args={'requester_pays': True}

target_options={"anon": True,
                "compression": "zstd"
                }
ref_storage_args = {}

target_protocol='http'
target_protocol='s3'

references = 's3://pangeo-data-uswest2/esip/adcirc/adcirc_01d_offsets.json'

mapper = fsspec.get_mapper("reference://", 
    references=references, 
    ref_storage_args=ref_storage_args, 
    target_protocol=target_protocol, 
    target_options=target_options)

ds = xr.open_dataset(mapper, engine="zarr", backend_kwargs={"consolidated": False})

ds = xr.open_zarr(mapper)
ds.nbytes/1e6   # MB
ds.nbytes/1e9   # GB
ds.nbytes/1e12  # TB

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
json_file = 's3://...json'
remote_options = {
    "key": response["accessKeyId"],
    "secret": response["secretAccessKey"],
    "token": response["sessionToken"],
    "client_kwargs": {"region_name": "us-west-2"},
}
remote_options = {anon:True}
ref_storage_args = {"requester_pays": True, "skip_instance_cache": True}
ref_storage_args = {"skip_instance_cache": True}

fs = fsspec.filesystem('reference', 
                       fo=json_file, 
                       remote_protocol='s3', 
                       ref_storage_args=ref_storage_args, 
                       remote_options=remote_options,
                       simple_templates=True)
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

# Hyrax NASA ? 
#-----------------------------------------------------------------------------.
#### Intake catalog 
# https://github.com/fsspec/kerchunk/blob/main/examples/intake_catalog.yml 

#-----------------------------------------------------------------------------.
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
        same_names=True)
fs_block = fsspec.implementations.cached.CachingFileSystem(
        fs=fs_s3,
        cache_storage=cache_storage_block,
        check_files=False,
        expiry_times=False,
        same_names=True)
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
            same_names=False)

    # satpy can't search recursively, only directly in the same directory
    # therefore use typhon, and filter channels manually later
    abi_fileset = FileSet(
            path=f"noaa-goes16/ABI-L1b-Rad{sector[0]:s}/"
                 "{year}/{doy}/{hour}/"
                 f"OR_ABI-L1b-Rad{sector:s}-M6C*_G16_"
                 "s{year}{doy}{hour}{minute}{second}*_e{end_year}{end_doy}"
                 "{end_hour}{end_minute}{end_second}*_c*.nc",
            name="abi",
            fs=fs_s3)
    files = list(fi for fi in abi_fileset.find(start_date, end_date) if any(f"C{c:>02d}_" in fi.path for c in chans))
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
scn = Scene(reader='abi_l1b', filenames=[url])
scn.load(['C14'])


# https://medium.com/pangeo/cloud-performant-reading-of-netcdf4-hdf5-data-using-the-zarr-library-1a95c5c92314
# Modified Zarr and xarray:  chunk_store to xr.open_zarr ,  FileChunkStore  to zarr
# chunkstore contains the chunk locations to enable the byte-range requests.
# Option 1
ncfile = ffspec.open('s3...', anon=False, requester_pays=True)

store = ffspec.get_mapper('s3... .nc.chunkstore', anom=True)
chunk_store = FileChunkStore(store, chunk_source=ncfile.open())

ds = xr.open_zarr(store, consolidated=True, chunk_store=chunk_store)


ds = xr.open_dataset(ncfile.open(), engine="h5netcdf", decode_times=False)

# Direct access via fs 
files = fs.glob(
    join("podaac-ops-cumulus-protected/", "MUR-JPL-L4-GLOB-v4.1", "2005*.nc")
)
ds = xr.open_dataset(file.open())
