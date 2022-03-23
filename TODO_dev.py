#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 11:04:53 2022

@author: ghiggi
"""
### Check consistency
# - Check for Mesoscale same location (on M1 and M2 separately) !
#   - FindFiles raise information when it changes !
# _check_unique_scan_mode: raise information when it changes
# _check_interval_regularity: raise info when missing between ... and ...
# --> Deal with mesoscale at 30 seconds? Now grouped within minutes? Maybe do not set seconds to 0?
# --> With check_consistency_False it download/find but then grouped within minute keys


### kerchunk
# - Parallelize kerchunk reference file creation efficiently
# - Benchmark dask.bag vs. dask_delayed vs. concurrent multithreading
# - MultiProcess seems to hang

# - replace bucket url in reference dictionary


# ds.nbytes / 1e6  # MB
# ds.nbytes / 1e9  # GB
# ds.nbytes / 1e12  # TB

# -----------------------------------------------------------------------------.
#### Download options

# CLI for download

# Download Daily Folders

# Download Monthly Products

##  Methods
# - rclone  <--- for bulk download ? how to parallelize
#   https://github.com/blaylockbk/pyBKB_v3/blob/master/rclone_howto.md

# rclone copy publicAWS:noaa-goes16/ABI-L2-MCMIPC/2018/283/00/OR_ABI-L2-MCMIPC-M3_G16_s20182830057203_e20182830059576_c20182830100076.nc ./

# - http download (wget, ... )

# -----------------------------------------------------------------------------.

# Turn some attributes to coordinates so that are preserved when concatenating
#  multiple GOES DataSets together.
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
