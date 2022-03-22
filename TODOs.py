#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 16:33:44 2022

@author: ghiggi
"""
 

# The L1b Radiances and L2 Cloud and Moisture Imagery have separate files for each of the 16 bands.
# 'ABI-L2-MCMIPC
# 'ABI-L1b-RadC',

# Full disk scans are available every 15 minutes
# CONUS scans are available every 5 minutes
# Mesoscale scans are available every minute

# M3: is mode 3 (scan operation),
# M4 is mode 4 (only full disk scans every five minutes – no mesoscale or CONUS)
## https://www.goes-r.gov/users/abiScanModeInfo.html 
# https://www.youtube.com/watch?v=qCAPwgQR13w&ab_channel=NOAASatellites

# Mode 3 
# - Till April 2, 2019 
# - FULL DISK every 15 minutes 
# - CONUS every 5 minutes

# Mode 3 Cooling Time (just for GOES-17 some periods of the year)
# --> M1 and M2 every 2 minutes
# --> CONUS not scanned 

# Mode 6 (difference between GOES-16  and GOES-17)
# - Since  April 2, 2019 
# - FULL DISK every 10 minutes 
# - CONUS every 5 minutes

# Mode 4 
# - Continuous 5-minute full disk imagery 
# - No mesoscale and CONUS produced 
# - October 1 2018
# --> Useful to test TIMEFRAME INTERPOLATION vs. OBSERVED

# Docs
# - https://docs.opendata.aws/noaa-goes16/cics-readme.html

# sunpy users

# JBRAVO EXAMPLE template
https://jhbravo.gitlab.io/geostationary-images/

#--------------------------------------------------------------------------.
### File size statistics

# full domain: 
# C02: 290 MB 
# C01: 45 MB 
# C03: 45 MB
# C06: 10 MB 
# C07-13 30 MB 
    
# Mesoscale domain: 295 KB per file !!!! 

# satpy.groupfile as an alternative to group by i.e. time 

# TODO: Precompute for each storage goes_api.get_product_availability_time(protocol, satellite, product)
# TODO: List start_time and end_time of a product 

# get_available_attributes

# kerchunked_object=True
# kerchunked_json_dir=None

### intake_catalog 

### CLI for download  

ds = xr.open_dataset(l_files[4])
ds['Rad'].shape
ds['Rad'].nbytes/1024/1024 # MB 
ds['Rad'].isel(x=slice(0,226), y=slice(0,226)).nbytes/1024/1024

####--------------------------------------------------------------------------.
#### Applications
# - intake-satpy 
# - kerchunk  json 
# - filename is a class wrapping kerchunk ... which has open() method implemented 
#   --> xarray need to be able to deal with kerchunk object ... 

# - get_file_availability(start_time, end_time)    # % 
# - get_file_unavailability(start_time, end_time)  # %

# - get_ts_attrs !!! 

####--------------------------------------------------------------------------.
# MCVE: Minimal Complete and Verifiable Example  --> Mimum Reproducible Example 


####--------------------------------------------------------------------------.
#### Existing python packages 
#### - GOES2GO
# https://github.com/blaylockbk/goes2go
# - IO:
#   https://github.com/blaylockbk/goes2go/blob/master/goes2go/data.py 
#   https://blaylockbk.github.io/goes2go/_build/html/reference_guide/index.html
# - CONFIG FILE
#   https://github.com/blaylockbk/goes2go/blob/master/goes2go/__init__.py
# - RGB composites via satpy
#   - https://github.com/blaylockbk/goes2go/blob/master/goes2go/accessors.py
#   - https://github.com/blaylockbk/goes2go/blob/master/goes2go/rgb.py

# - wind tools : https://blaylockbk.github.io/goes2go/_build/html/user_guide/notebooks/DEMO_derived_motion_winds.html
# - GLM: https://blaylockbk.github.io/goes2go/_build/html/user_guide/notebooks/field-of-view_GLM.html 
#       https://blaylockbk.github.io/goes2go/_build/html/user_guide/notebooks/field-of-view_GLM_Edges.html

goes2go.data.goes_latest(*, satellite='noaa-goes16', 
                         product='ABI-L2-MCMIP',
                         domain='C', return_as='xarray',
                         download=True, overwrite=False, 
                         save_dir=PosixPath('/p/cwfs/blaylock/data'), 
                         s3_refresh=True, verbose=True)


# Get latest data
G_ABI = goes_latest(satellite='G16', product='ABI')
# Get data for a specific time
G_GLM = goes_nearesttime('2021-01-01 12:00', satellite='G16', product='GLM')

### goes-2-go
# https://github.com/blaylockbk/goes2go

# https://github.com/joaohenry23/GOES

# https://github.com/mnichol3/goesaws

# https://github.com/palexandremello/goes-py

### goes-downloader (has a UI)
# https://github.com/uba/goes-downloader

### goes-mirror (JUSSI)
# https://github.com/meteoswiss-mdr/goesmirror/blob/master/goesmirror/goesmirror.py

### goesaws CLI (query and download)
# https://github.com/mnichol3/goesaws
# https://github.com/mnichol3/goesaws/blob/master/goesaws/goesawsinterface.py

####--------------------------------------------------------------------------.
### Plot image from GIBBS 
# - https://www.ncdc.noaa.gov/gibbs/availability/2022-01-10
# - https://github.com/space-physics/GOESplot/blob/main/src/goesplot/io.py

####--------------------------------------------------------------------------.
#### GOES Products & useful links 
# https://www.goes-r.gov/products/overview.html
# https://rammb.cira.colostate.edu/training/visit/quick_guides/ 

# Useful links: 
# https://blaylockbk.github.io/goes2go/_build/html/

####--------------------------------------------------------------------------.
#### Cloud buckets 
## gcp public data goes 
# https://console.cloud.google.com/storage/browser/gcp-public-data-goes-17
# https://console.cloud.google.com/storage/browser/gcp-public-data-goes-16 

# ## noaa aws
# https://noaa-goes17.s3.amazonaws.com/index.html
# https://noaa-goes16.s3.amazonaws.com/index.html

# # oracle 
# https://opendata.oraclecloud.com/ords/r/opendata/opendata/details?data_set_id=2&clear=CR,8&session=2142808942446

# # microsoft (only limited set)
# https://planetarycomputer.microsoft.com/dataset/goes-cmi
# Blob Storage in the West Europe Azure data center
# https://planetarycomputer.microsoft.com/dataset/goes-cmi#Storage-Documentation 
#  Data list 
# https://github.com/microsoft/AIforEarthDataSets/blob/main/data/goes-r.md#storage-resources
# https://planetarycomputer.microsoft.com/dataset/goes-cmi

'gs://gcp-public-data-goes-16/ABI-L1b-RadF/2017/258/18/OR_ABI-L1b-RadF-M3C14_G16_s20172581800377_e20172581811144_c20172581811208.nc' 
"s3://noaa-goes16/ABI-L1b-RadF/2019/321/14/OR_ABI-L1b-RadF-M6C01_G16_s20193211440283_e20193211449591_c20193211450047.nc"
 # Amazon S3 bucket in the us-east-1 AWS region. 

# Grants and free trials info 
# - https://www.ncei.noaa.gov/access/cloud-access


# NOAA Global Hydro Estimator (GHE)
# https://microsoft.github.io/AIforEarthDataSets/data/ghe.html
# https://registry.opendata.aws/noaa-ghe/

# GHCN-D
# https://registry.opendata.aws/noaa-ghcn/ 
# ttps://console.cloud.google.com/marketplace/product/noaa-public/ghcn-d?filter=solution-type:dataset&q=NOAA&id=9d500d1d-fda4-4413-a789-d8786fd6592e&pli=1


'gs://gcp-public-data-goes-16/ABI-L1b-RadF/2017/258/18/OR_ABI-L1b-RadF-M3C14_G16_s20172581800377_e20172581811144_c20172581811208.nc' 
"s3://noaa-goes16/ABI-L1b-RadF/2019/321/14/OR_ABI-L1b-RadF-M6C01_G16_s20193211440283_e20193211449591_c20193211450047.nc"
 
# # EU west 
# https://goeseuwest.blob.core.windows.net/noaa-goes16
# https://goeseuwest.blob.core.windows.net/noaa-goes17


# https://goes.blob.core.windows.net/noaa-goes16 
# https://goes.blob.core.windows.net/noaa-goes17

# Examples: 
# - https://planetarycomputer.microsoft.com/dataset/goes-cmi#Blob-Storage-Notebook 
# 

# # GOES Azure 
# from azure.storage.blob import ContainerClient
# storage_account_url = 'https://goeseuwest.blob.core.windows.net'
# container_name = 'noaa-goes16'
# goes_blob_root = storage_account_url + '/' + container_name + '/'
# goes_container_client = ContainerClient(account_url=storage_account_url, container_name=container_name, credential=None)
# generator = goes_container_client.list_blobs(name_starts_with=prefix)

# import fsspec 
# fs = fsspec.open("http://goeseuwest.blob.core.windows.net/noaa-goes16")
# fs = fsspec.filesystem("http://goeseuwest.blob.core.windows.net/noaa-goes16")
# fs.ls("goeseuwest.blob.core.windows.net/noaa-goes16")
 
# fs = fsspec.filesystem('abfs')
# fs.ls("http://goeseuwest.blob.core.windows.net/noaa-goes16")                
# fs = 
# adlfs
# # adlfs

# https://github.com/fsspec
# s3fs # instead of boto3 
# https://s3fs.readthedocs.io/en/latest/
# gcsfs # instead of ... ? 
# https://gcsfs.readthedocs.io/en/latest/index.html
# adlfs
# https://github.com/fsspec/adlfs 

import xarray as xr
import requests
import netCDF4
import boto3
def get_s3_keys(bucket, prefix = ''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix):
                yield key
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
                                         
keys = get_s3_keys(bucket_name, prefix = prefix)

resp = requests.get(filepath)
file_name = key.split('/')[-1].split('.')[0]

nc4_ds = netCDF4.Dataset(file_name, memory = resp.content)
store = xr.backends.NetCDF4DataStore(nc4_ds)
DS = xr.open_dataset(store)

##----------------------------------------------------------------------------.
#### QUery data from https 
# https://stackoverflow.com/questions/11023530/python-to-list-http-files-and-directories 


#### Download via boto3 
## To reduce bandwidth usage, reduce max_concurrency 
#          to increase usage, increase max_concurrency
# Threads to implement concurrency
max_concurrency=5
config = TransferConfig(use_threads=True,
                        max_concurrency = max_concurrency)



# Download an S3 object
s3 = boto3.client('s3')
s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME', Config=config)
 
## boto3 _download_file 
s3_client = boto3.resource('s3')
my_bucket = s3_client.Bucket(bucket_name)
my_bucket.download_file(key, filename)
 
s3_client = boto3.client('s3')
s3_client.download_file(bucket_name, key, filename)
 
# Resources 
# - High level service class
# - High level clients are not thread safe.
# - Initiate a client in each thread
s3 = boto3.resource('s3') 

# Client
# - Low-level service class
# - Low level clients are thread safe. 
# - When using a low-level client, it is recommended to instantiate your client 
#   then pass that client object to each of your threads.
s3 = boto3.client('3')
s3 = boto3.resource('s3')
            
# Customized Session 
# - Initiate AWS connectivity
session = boto3.session.Session()
s3 = session.resource('s3') 
s3 = session.client('3')
s3 = session.resource('s3')