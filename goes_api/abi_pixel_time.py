#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 14:29:21 2022

@author: ghiggi
"""
import os
import xarray as xr 
from goes_api.io import (
    _check_satellite,
    _check_scan_mode,
    _check_sector
)
    
    
def _get_LUT_dict(): 
    LUT_dict = {
       # G17 
       "Mode6M": "340_Timeline_05M_Mode6_v2.7.nc",                # G17 (Mode6M)
       "Mode6I": "ABI-Timeline03I_Mode6_Cooling_hybrid.nc",    # G17 cooling (equal to G17 Mode6M for Full Disc, but no CONUS)
       "Mode3G": "ABI-Timeline03G_Mode3_Cooling_ShortStars.nc",   # G17 cooling (Mode 3G)

       # G16
       "Mode6A": "ABI-Timeline05B_Mode 6A_20190612-183017.nc", # G16 (Mode 6A)
       "Mode6F": "Timeline05F_Mode6_20180828-092941.nc",       # G16 (Mode 6F) (for FD differ of 3 sec in last 2 swath)
       "Mode3": "Timeline03C_Mode3_20180828-092953.nc",
       "Mode4": "ABI-Timeline04A_Mode 4_20181219-104006.nc",
      }
    return LUT_dict

def _get_LUT_filepath(satellite, scan_mode): 
    # Get LUT filename
    LUT_dict = _get_LUT_dict() 
    if satellite == "goes-16":
        if scan_mode == "M6": 
            fname = LUT_dict['Mode6A']
        elif scan_mode == "M3": 
            fname = LUT_dict['Mode3']
        # M4
        else: # scan_mode == "M4":
            fname = LUT_dict['Mode4']
    elif satellite == "goes-17": 
        if scan_mode == "M6": 
            fname = LUT_dict['Mode6M']
        elif scan_mode == "M3": 
            fname = LUT_dict['Mode3G']
        # M4
        else: # scan_mode == "M4":
            fname = LUT_dict['Mode4']
    else:
        raise NotImplementedError("Available only for GOES-16 and GOES-17.")
        
    # Retrieve package fpath 
    package_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Define LUT fpath 
    fpath = os.path.join(package_dir, "data", "ABI-Time_Model_LUTS", fname)
    return fpath     
        
def _get_native_pixel_time_offset(satellite, scan_mode, resolution, sector):
    # Check inputs 
    satellite = _check_satellite(satellite)
    scan_mode = _check_scan_mode(scan_mode)
    sector = _check_sector(sector, sensor='ABI')
 
    # Open LUT pixel offset 
    fpath = _get_LUT_filepath(satellite, scan_mode)
    ds = xr.open_dataset(fpath)
    
    # Retrieve sector pixel offset 
    if sector == "F":
        da = ds["FD_pixel_times"]
        da = da.swap_dims({"FD_cols": 'x', "FD_rows": 'y'}).transpose('y','x')
    elif sector == "C": 
        da = ds["CONUS1_pixel_times"]
        da = da.swap_dims({"CONUS1_cols": 'x', "CONUS1_rows": 'y'}).transpose('y','x')
    elif sector == "M": 
        da = ds["MESO_pixel_times"]
        da = da.swap_dims({"MESO_cols": 'x', "MESO_rows": 'y'}).transpose('y','x')  
    else:
        raise NotImplementedError("Available only for sector F, C and M.")
        
    # Scale based on resolution
    if resolution == "500m":
       arr = da.data.repeat(4, axis = 0).repeat(4, axis = 1)
       da =  xr.DataArray(arr, dims=['y','x'])
       da.name = resolution
    elif resolution == "1km":
       arr = da.data.repeat(2, axis = 0).repeat(2, axis = 1)
       da =  xr.DataArray(arr, dims=['y','x'])
       da.name = resolution
    else: # resolution 2km   
       da.name = resolution
    # Remove attributes
    da.attrs = {}
    # Return pixel offset 
    return da 

def _get_pixel_time_offset(satellite, scan_mode, resolution, sector):
    # Check inputs 
    satellite = _check_satellite(satellite)
    scan_mode = _check_scan_mode(scan_mode)
    sector = _check_sector(sector, sensor='ABI')
    
    # Retrieve package fpath 
    package_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Define LUT fpath 
    fname = "_".join([satellite, sector, scan_mode, resolution]) + ".nc"
    fpath = os.path.join(package_dir, "data", "ABI_Pixel_TimeOffset", fname)
    
    # Open file 
    ds = xr.open_dataset(fpath)
    # Convert to timedelta 
    for var in ds.data_vars:
        ds[var] = ds[var].astype('m8[s]')
    # Return data
    return ds
    


# from goes_api.abi_pixel_time import _get_native_pixel_time_offset
# satellite = "G16"
# sector = "F"
# scan_mode = "M3"
# resolution = "500m"

 
# t_i = time.time()
# da = _get_native_pixel_time_offset(satellite=satellite,
#                            sector=sector, 
#                            scan_mode=scan_mode, 
#                            resolution=resolution)

# t_f = time.time()
# t_f-t_i

     