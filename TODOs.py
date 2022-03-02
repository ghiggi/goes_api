#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 16:33:44 2022

@author: ghiggi
"""
 
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


### GOES CLI (query and download)
# https://github.com/mnichol3/goesaws

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

# import fsspec 
# fs = fsspec.open("http://goeseuwest.blob.core.windows.net/noaa-goes16")
# fs = fsspec.filesystem("http://goeseuwest.blob.core.windows.net/noaa-goes16")
# fs.ls("goeseuwest.blob.core.windows.net/noaa-goes16")
 
# fs = fsspec.filesystem('abfs')
# fs.ls("http://goeseuwest.blob.core.windows.net/noaa-goes16")                
# fs = 
# adlfs
# # adlfs

##----------------------------------------------------------------------------.
#### QUery data from https 
# https://stackoverflow.com/questions/11023530/python-to-list-http-files-and-directories 
