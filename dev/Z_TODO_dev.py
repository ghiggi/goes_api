#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 11:04:53 2022

@author: ghiggi
"""
# -----------------------------------------------------------------------------.
### Colab Tutorials 
# https://colab.research.google.com/drive/1RkS4HpbVUtNZ6UQm4-bvEQpp7jXXB3lJ?usp=sharing#scrollTo=u0kc-GmGRUIl


# -----------------------------------------------------------------------------.

# TODO: Build time series of attributes (coords of xarray)
# --> Use small band file for ABI
# - nominal_satellite_subpoint_lon
# - nominal_satellite_subpoint_lat
# - nominal_satellite_subpoint_height
# - scan mode 
# - percent_uncorrectable_L0_errors


# TODO: Plot scan modes offsets 

# TODO: Identify 30 seconds imagery

# TODO: Precompute for each storage goes_api.get_product_availability_time(protocol, satellite, product)

# TODO: List start_time and end_time of a product

# TODO: GOES_ABI DAILY DISK STORAGE AMOUNT

# - get_file_availability(start_time, end_time)    # %
# - get_file_unavailability(start_time, end_time)  # %

# Look at availability 
# - https://qcweb.ssec.wisc.edu/web/abi_quality_scores/
# - https://qcweb.ssec.wisc.edu/web/abi_quality_scores/#date:2020-06-28;sat:GOES-17;

# TODO: in GOES Package 
# -'get_lonlatcorner','corner_size_to_center_size',
#  'midpoint_in_x','midpoint_in_y','calculate_corners',
# - find_pixel_of_coordinate 

#------------------------------------------------------------------------------.
### Get chunk info() 
# File memory info
# ds = xr.open_dataset(l_files[4])
# ds['Rad'].shape
# ds['Rad'].nbytes/1024/1024 # MB
# ds['Rad'].isel(x=slice(0,226), y=slice(0,226)).nbytes/1024/1024

#------------------------------------------------------------------------------.
# TODO: 
# - Satpy composite to Zarr
# - Multiscale Zarr (pyramids)
# - Open in Napari 

#------------------------------------------------------------------------------.
# TODO: 
# - Add quickguides: http://cimss.ssec.wisc.edu/goes/GOESR_QuickGuides.html 

#------------------------------------------------------------------------------.
### Check consistency
# - Check for Mesoscale same location (on M1 and M2 separately) !

# --> Deal with mesoscale at 30 seconds? Now grouped within minutes? Maybe do not set seconds to 0?
# --> With operational_checks_False it download/find but then grouped within minute keys

# TODO: utility to search when scan_mode changes

#------------------------------------------------------------------------------.
### Optimize kerchunk utils
# - Parallelize kerchunk reference file creation efficiently
# - Benchmark dask.bag vs. dask_delayed vs. concurrent multithreading
# - MultiProcess seems to hang

# - replace bucket url in reference dictionary

# ds.nbytes / 1e6  # MB
# ds.nbytes / 1e9  # GB
# ds.nbytes / 1e12  # TB

# -----------------------------------------------------------------------------.
#### Download options utils 

# CLI for download

# Download Daily Folders

# Download Monthly Products

##  Methods
# - rclone  <--- for bulk download ? how to parallelize
#   https://github.com/blaylockbk/pyBKB_v3/blob/master/rclone_howto.md

# rclone copy publicAWS:noaa-goes16/ABI-L2-MCMIPC/2018/283/00/OR_ABI-L2-MCMIPC-M3_G16_s20182830057203_e20182830059576_c20182830100076.nc ./

# - http download (wget, ... )

# -----------------------------------------------------------------------------.
### When concatenating GOES datasets 
# Turn some attributes to coordinates so that are preserved when concatenating
#  multiple netCDFs together.
attr2coord = [
    "dataset_name",
    "date_created",
    "time_coverage_start",
    "time_coverage_end",
]

def _attrs_2_coords(ds, attrs):
    for key in attrs:
        if key in ds.attrs:
            ds.coords[key] = ds.attrs.pop(key)
    return ds

####--------------------------------------------------------------------------.
#### - GOES2GO RGB composites via satpy
# - https://github.com/blaylockbk/goes2go/blob/master/goes2go/accessors.py
# - https://github.com/blaylockbk/goes2go/blob/master/goes2go/rgb.py

# - wind tools : https://blaylockbk.github.io/goes2go/_build/html/user_guide/notebooks/DEMO_derived_motion_winds.html

# JBRAVO EXAMPLE template
# https://jhbravo.gitlab.io/geostationary-images/

####--------------------------------------------------------------------------.
### Plot image from GIBBS
# - https://www.ncdc.noaa.gov/gibbs/availability/2022-01-10
# - https://github.com/space-physics/GOESplot/blob/main/src/goesplot/io.py

####--------------------------------------------------------------------------.
### VECTORIZE: Create contours shapefiles from raster grid
# - Grid --> Contours --> Shapefile (--> Tracking)
# https://github.com/daniellelosos/GOES-R_NetCDF_to_Shapefile/blob/main/Continuous%20variable%20to%20contour%20to%20shapefile.ipynb

# - Binary Grid --> Shapefile 
# https://github.com/daniellelosos/GOES-R_NetCDF_to_Shapefile/blob/main/Discrete%20variable%20to%20shapefile.ipynb
