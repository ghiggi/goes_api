#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 16:33:44 2022

@author: ghiggi
"""
# https://github.com/pytroll/pytroll-examples/issues/35
# Post performance benchmark
# Correct cgentlemann, file and chunked and very small !!!

# Add https://github.com/pytroll/satpy/issues/1062
# https://github.com/pytroll/satpy/pull/1423

# sunpy users

# sudo apt install nethogs
# nethogs -v 3 to monitor data transfer

# todo: rerun benchmark with blockcache
# --->  need to optimize block_size
# --->  use simplecache
# - https://github.com/pytroll/satpy/pull/1321

# TODO: GOES_ABI DAILY DISK STORAGE AMOUNT

# kerchunk with satpy

# TCW - Not useful because only in clear-sky ?

# GLM: https://blaylockbk.github.io/goes2go/_build/html/user_guide/notebooks/field-of-view_GLM.html
#      https://blaylockbk.github.io/goes2go/_build/html/user_guide/notebooks/field-of-view_GLM_Edges.html

# --------------------------------------------------------------------------.

# Plot scan modes types

# Identify 30 seconds imagery

# GOES ABI MODE 4 : useful to test timeframe interpolation

# JBRAVO EXAMPLE template
# https://jhbravo.gitlab.io/geostationary-images/


# --------------------------------------------------------------------------.
### Other ideas
# TODO: Precompute for each storage goes_api.get_product_availability_time(protocol, satellite, product)

# TODO: List start_time and end_time of a product

# TODO: Build time series of attributes (coords of xarray)

# File memory info
# ds = xr.open_dataset(l_files[4])
# ds['Rad'].shape
# ds['Rad'].nbytes/1024/1024 # MB
# ds['Rad'].isel(x=slice(0,226), y=slice(0,226)).nbytes/1024/1024

####--------------------------------------------------------------------------.
#### Applications
# - intake-satpy
# - kerchunk json

# - filename is a class wrapping kerchunk ... which has open() method implemented
#   --> xarray need to be able to deal with kerchunk object ...

# - get_file_availability(start_time, end_time)    # %
# - get_file_unavailability(start_time, end_time)  # %

####--------------------------------------------------------------------------.
# MCVE: Minimal Complete and Verifiable Example  --> Mimum Reproducible Example

####--------------------------------------------------------------------------.
#### - GOES2GO RGB composites via satpy
# - https://github.com/blaylockbk/goes2go/blob/master/goes2go/accessors.py
# - https://github.com/blaylockbk/goes2go/blob/master/goes2go/rgb.py

# - wind tools : https://blaylockbk.github.io/goes2go/_build/html/user_guide/notebooks/DEMO_derived_motion_winds.html

####--------------------------------------------------------------------------.
### Plot image from GIBBS
# - https://www.ncdc.noaa.gov/gibbs/availability/2022-01-10
# - https://github.com/space-physics/GOESplot/blob/main/src/goesplot/io.py

####--------------------------------------------------------------------------.
