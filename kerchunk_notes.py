#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 18:54:28 2022

@author: ghiggi
"""
# -----------------------------------------------------------------------------.
####################
##### KERCHUNK #####
####################
# - Formerly known as fsspec-reference-maker
# - A kind of virtual file-system for fsspec
# - Enable to access the contents of binary files directly without the limitations
#    of the package designed for that file type

# - It derive a single metadata JSON file describing all chunk locations of a NetCDF4/HDF5 file  
# - It extract the internal references of a netCDF4/HDF5 file
# - It enable to perform efficient byte-range requests.
# - The derived JSON metadata file can point directly to 
#   where the required data are, instead having to access all files to 
#   understand which are necessary.
# - Builds upon the chunk query API feature introduced in the HDF5 1.10.5 library. [CHECK]


# NetCDF4/HDF5 files were designed for filesytem use
# - Suffer of performance issues when accessed from cloud object storage
# - Does not allow concurrent multi-threaded reads

# Zarr was specifically designed to overcome such issues
# --> Metadata are stored in a single JSON object
# --> Each chunk is a separate object
# --> Enable unlimited file sizes and parallel writes/reads
# --> Avoid HDF5/netCDF4 library issues with concurrent multi-threaded reads.

# HDF5/NetCDF files could be directly opened from s3 but performance is poor
#  because: 
#  1. HDF5/NetCDF library executes many small reads 
#  2. s3 storage has significantly higher latency than traditional file systems

###---------------------------------------------------------------------------.
# kerchunk JSON metadata standards
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

###---------------------------------------------------------------------------.
### async
# - Will give a speed-up if 
#    1. Multiple chunks are being read at once 
#       ---> The dask partition (chunk size) is larger than the file chunksize by some factor 
#    2. The latency/overhead of each request is a significant fraction of the overall time.
# - async lessens the overhead for smaller file chunks (so long as they are loaded concurrently,
#   meaning a dask partition (chunk) containing many file chunks)
# - Once the bandwidth limits are reached, as opposed to waiting, then async doesn’t help.
# - s3fs and gcsfs now internally load the small chunks asynchronously inside each task.

# - Should typically help when we encounter a dataset in object storage with lots 
#   of little chunks.
# - Should typically help not so much when we read datasets with 100 MB chunks 

# - One consequence of the async capability is that there is less of a performance penalty
#   for using smaller Zarr chunks. You still want to use Dask chunks of ~100MB for large compute jobs.
#   But the underlying file chunks can be smaller.

###---------------------------------------------------------------------------.
### Considerations related to dask chunk size
# - May not perform well if the dask chunk size are too small.
# - If dask chunk sizes are less than a few MB, the read times could drop to less than 100ms 
#   --> https://distributed.dask.org/en/latest/limitations.html#limitations
# - Dask chunk sizes in the range of 10–200MB work well with Dask reading from Zarr data in cloud storage
# ---> Golden rule:  Dask chunk size > File chunk size (and aligned possibly) !!!

###---------------------------------------------------------------------------.
### Cloud Bucket Best Practices
# Amazon’s Best Practices for S3 recommends making concurrent requests 
#   for byte ranges of an object at the granularity of 8–16 MB 

# Maximum request rate limit
# - Amazon S3: 6000/s.
# - GCP: 500/s
# - Microsoft Azure Blob Storage: 500/s

###---------------------------------------------------------------------------.
### Request Latency 
# - How much takes each request (Signell: 350ms)
# --> Dask profiler ! TODO
 
### JSON File Size 
# - TODO How much does it size a JSON? How much does it take to create?

### Steps 
# 1. Read the JSON metadata file into an fsspec.referenceFileSystem class 
# 2. Create a mapper
# 3. Read the mapper using the Zarr library.


###---------------------------------------------------------------------------.
### Documentation 
# - https://github.com/fsspec/kerchunk 
# - https://fsspec.github.io/kerchunk/index.html 
# - https://fsspec.github.io/kerchunk/cases.html

### Applications PPT
# - https://medium.com/pangeo/fake-it-until-you-make-it-reading-goes-netcdf4-data-on-aws-s3-as-zarr-for-rapid-data-access-61e33f8fe685
# - https://lucassterzinger.com/assets/siparcs-2021/Sterzinger_Lucas_Slides.pdf
# --> 24 HOURS GOES16 ABI L2-MCMIPF --> 416 MB of JSON instead of 52 GB of files 

###---------------------------------------------------------------------------.
### GOES tutorial
# https://github.com/lsterzinger/fsspec-reference-maker-tutorial
# https://github.com/lsterzinger/fsspec-reference-maker-tutorial/blob/main/tutorial.ipynb 
# https://github.com/lsterzinger/cloud-optimized-satellite-data-tests
 
#---------------------------------------------------------------------------.
# Another kerchunk tutorial
# https://github.com/lsterzinger/2022-esip-kerchunk-tutorial
# https://github.com/lsterzinger/2022-esip-kerchunk-tutorial/blob/main/01-Create_References.ipynb !!!





###---------------------------------------------------------------------------.
# Chelle Gentemann (SST-Tutorial)
# https://nbviewer.org/github/cgentemann/cloud_science/blob/master/zarr_meta/cloud_mur_v41_benchmark.ipynb

###---------------------------------------------------------------------------.
# Richard Signell (USGS)
# https://medium.com/pangeo/cloud-performant-reading-of-netcdf4-hdf5-data-using-the-zarr-library-1a95c5c92314
# https://medium.com/pangeo/cloud-performant-netcdf4-hdf5-with-zarr-fsspec-and-intake-3d3a3e7cb935

###---------------------------------------------------------------------------.
## Other snippets:
# - https://gist.github.com/rsignell-usgs/ef435a53ac530a2843ce7e1d59f96e22 # file-nwm_forecast_make_json_parallel-ipynb
# - https://gist.github.com/rsignell-usgs/6c6585cb2b52c6385720009fd30c0519 # Efficient access to the NWM medium

# - https://gist.github.com/rsignell-usgs/b12d6b8c91fb3e933bd0b0c2868cb3fd # ERA5 !!!
# - https://gist.github.com/rsignell-usgs/413b2a31aec1641b10f1250937262e05
# - https://gist.github.com/rsignell-usgs/da74d35ce28979babe7338ca698108c5 # ERA5
# - https://gist.github.com/rsignell-usgs/378be399f3c065fc5b90e503faf3e64d # ERA5 local

# - https://gist.github.com/rsignell-usgs/6117f63ba1b266ea713249b98492ce76 # GRIDMET
# - https://gist.github.com/rsignell-usgs/40cd834d44bfa7c0e8621a8b81adb53a # GRIDMET

# - https://gist.github.com/rsignell-usgs?page=2

###---------------------------------------------------------------------------.
# Combine 3 kerchunked datasets
# - https://gist.github.com/rsignell-usgs/1ca99bbda72f70dccdf86976d7279a08

# MultiZarrToZarr: kwargs
# - https://github.com/lsterzinger/fsspec-help/blob/main/example.ipynb
# - https://github.com/lsterzinger/fsspec-reference-maker-tutorial/blob/main/tutorial.ipynb
###---------------------------------------------------------------------------.




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

#### Option 2

file_location = 's3://....'
ikey = ffspec.get_mapper(file_location, anon=True)
xr.open_zarr(ikey, consolidated=True) 


from fsspec_reference_maker.hdf import Hdf5ToZarr

s3_fn = 's3://' + s3_fpath_nc
references = "./refs" + os.path.basename(s3_fn).replace(".nc",".json")

with ffsspec.open(s3_fn,
                 anon=True, 
                 mode="rb", 
                 default_fill_chache=False, 
                 default_cache_type='none') as f: 
    h5chunks = Hdf5ToZarr(f, s3_fn, xarray=True)
    json_str = h5chunks.translate() 
with open(references, 'wt') as f: 
    json.dump(json_str, f, indent=4) 

# open_zarr_fun() 
target_options_dict = {'anon':True, 
                       'skip_instance_cache':True,
                       'use_listings_cache': False }

m =  ffspec.get_mapper("reference://", 
                       target_protocol="s3",
                       references=referece,
                       target_options=target_options_dict)
ds = xr.open_zarr(m) 

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