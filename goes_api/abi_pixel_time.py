#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Ghiggi Gionata

# goes_api is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# goes_api is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# goes_api. If not, see <http://www.gnu.org/licenses/>.
"""Define functions to retrieve ABI pixel scan time."""

import os
import datetime
import numpy as np 
import xarray as xr 
from goes_api.checks import (
    _check_satellite,
    _check_scan_mode,
    _check_sector,
)
from goes_api.abi_utils import (
    get_sector_from_attrs,
    get_scan_mode_from_attrs,
    get_resolution_from_str,
    get_resolution_from_attrs,
)

# TODO: DOWNLOAD GOES-API LUTS USING POOCH 

#################################
#### GOES-API LUT GENERATION ####
#################################

def _get_lut_dict():
    """Get GOES ABI scan time offset LUT dictionary."""
    LUT_dict = {
       # G17 
       "Mode6M": "340_Timeline_05M_Mode6_v2.7.nc",                # G17 (Mode6M)
       "Mode6I": "ABI-Timeline03I_Mode6_Cooling_hybrid.nc",       # G17 cooling (equal to G17 Mode6M for Full Disc, but no CONUS)
       "Mode3G": "ABI-Timeline03G_Mode3_Cooling_ShortStars.nc",   # G17 cooling (Mode 3G)

       # G16
       "Mode6A": "ABI-Timeline05B_Mode 6A_20190612-183017.nc", # G16 (Mode 6A)
       "Mode6F": "Timeline05F_Mode6_20180828-092941.nc",       # G16 (Mode 6F) (for FD differ of 3 sec in last 2 swath)
       "Mode3": "Timeline03C_Mode3_20180828-092953.nc",
       "Mode4": "ABI-Timeline04A_Mode 4_20181219-104006.nc",
      }
    return LUT_dict

def _get_lut_filepath(satellite, scan_mode):
    """Get GOES ABI scan time offset LUT filepath."""
    # Get LUT filename
    LUT_dict = _get_lut_dict() 
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
    """Get GOES ABI pixel time offset DataArray."""
    # Check inputs 
    satellite = _check_satellite(satellite)
    scan_mode = _check_scan_mode(scan_mode)
    sector = _check_sector(sector, sensor='ABI')
    resolution = str(resolution)
    if resolution not in ["500", "1000", "2000"]:
        raise ValueError("Valid nadir 'resolution' are '500','1000' and '2000' m.")
    
    # Open LUT pixel offset 
    fpath = _get_lut_filepath(satellite, scan_mode)
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
    if resolution == "500":
       arr = da.data.repeat(4, axis = 0).repeat(4, axis = 1)
       da =  xr.DataArray(arr, dims=['y','x'])
       da.name = resolution
    elif resolution == "1000":
       arr = da.data.repeat(2, axis = 0).repeat(2, axis = 1)
       da =  xr.DataArray(arr, dims=['y','x'])
       da.name = resolution
    else: # resolution == 2000  
       da.name = resolution
    
    # Remove attributes
    da.attrs = {}
    # Return pixel offset 
    return da


####--------------------------------------------------------------------------.
######################
#### GOES-API LUT ####
######################
# Saving the 1km LUT as timedelta takes 898 MBs (and <0s to read)
# Saving the 1km LUT as uint takes 1.7 MBs (but about 6s to read)


def get_lut_filepath(satellite, sector, scan_mode, resolution):
    """Get GOES ABI pixel time offset LUT filepath."""
    # Retrieve package fpath 
    package_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Define LUT fpath 
    satellite = satellite.lower()
    resolution = str(resolution)
    fname = "_".join([satellite, sector, scan_mode, resolution]) + ".nc"
    fpath = os.path.join(package_dir, "goes_api", "data", "ABI_Pixel_TimeOffset", fname)
    return fpath
    
def get_pixel_time_offset(satellite, sector, scan_mode, resolution):
    """Get GOES ABI pixel time offset DataArray."""
    # Check inputs 
    satellite = _check_satellite(satellite)
    scan_mode = _check_scan_mode(scan_mode)
    sector = _check_sector(sector, sensor='ABI')
    
    # Retrieve LUT fpath
    fpath = get_lut_filepath(satellite, sector, scan_mode, resolution)
 
    # Open file 
    ds = xr.open_dataset(fpath)  # < 0s
    var = list(ds.data_vars)[0]
    da = ds[var].astype('m8[s]') # THIS CONVERSION TAKES ABOUT 6 SECONDS !!!
    da.name = "ABI_pixel_time_offset"
    # Return data
    return da


def get_abi_pixel_time(data):
    """Get GOES ABI pixel scan time DataArray."""
    if not isinstance(data, (xr.Dataset, xr.DataArray)): 
        raise TypeError("Provide xr.Dataset or (satpy scene) xr.DataArray.")
    
    # Define attributes to keep 
    attrs_keys = [# satpy 
                  'orbital_parameters',
                  'area',
                  'resolution', 
                  'grid_mapping', 
                  # 'cell_methods',
                  'platform_name',
                  'platform_shortname',
                  'orbital_slot',
                  'sensor',
                  'scan_mode',
                  'start_time',
                  'end_time',
                  # L1B/L2 products 
                  "platform_ID",
                  "instrument_type",
                  "timeline_id",
                  "spatial_resolution",
                  "time_coverage_start",
                  "time_coverage_end", 
                  ]
    attrs = data.attrs.copy()
    # If satpy DataArray
    if isinstance(data, xr.DataArray):
        try: 
            satellite = attrs['platform_name']
            scan_mode = attrs['scan_mode']
            sector = attrs['scene_abbr'][0]
            resolution = get_resolution_from_str(attrs['area'].description)
            start_time = attrs['start_time']  
            # end_time = attrs['end_time']  
        except:
            raise TypeError("The provided xr.DataArray must be extracted by "
                            "a satpy scene object using scn['<channel>'].")

    # Else if ABI L1B or L2 product         
    else:   
      try: 
          satellite = attrs['platform_ID']
          scan_mode = get_scan_mode_from_attrs(attrs)
          sector = get_sector_from_attrs(attrs)
          resolution = get_resolution_from_attrs(attrs)
          start_time = datetime.datetime.fromisoformat(attrs['time_coverage_start'][:-3])
          # end_time = datetime.datetime.fromisoformat(attrs['time_coverage_end'][:-3])
          # # Extract first variable (which should be 'Rad' or the 'L2' product)
          # var = list(data.data_vars)[0]
          # data = data[var]
      except:
          raise TypeError("The provided xr.Dataset seems not an ABI L1B or L2 file. "
                          "Please report the issue.")  
    
    # Check resolution 
    resolution = str(resolution)
    if resolution not in ['500', '1000', '2000']:
        raise NotImplementedError("ABI pixel scan time implemented only for "
                                  "500, 1000 and 2000 m nadir resolution.")
    
    # Retrieve time offset 
    time_offset = get_pixel_time_offset(satellite=satellite, 
                                        sector=sector,
                                        scan_mode=scan_mode, 
                                        resolution=resolution, 
                                        )
    # Compute pixel scan time 
    pixel_time = time_offset + np.datetime64(start_time)
    pixel_time.name = "ABI_pixel_time"
    
    # Add file coordinates 
    pixel_time = pixel_time.assign_coords(data.coords)
    
    # Do not check latest pixel time < file end_time
    # - Would require masking Full Disc which is computationally inefficient
    # assert pixel_time.max() < np.datetime64(end_time)
    
    # Add attributes 
    new_attrs = {k: attrs[k] for k in attrs_keys if attrs.get(k, None) is not None}
    new_attrs['long_name'] = "ABI Pixel Scan Time"
    new_attrs['standard_name'] = "pixel_scan_time"
    new_attrs['description'] = "Time of pixel acquisition."
    new_attrs['comments'] = "Scan time offset look up table derived from https://www.star.nesdis.noaa.gov/GOESCal/goes_tools.php . The pixel scan time maximum error is 40 s (at ABI scan swath edges)."
    new_attrs['history'] = "Created by ghiggi/goes_api"
    new_attrs['software'] = "https://github.com/ghiggi/goes_api"
    pixel_time.attrs = new_attrs
    
    # Return pixel_time DataArray
    return pixel_time
    

#------------------------------------------------------------------------------.
# from goes_api.abi_pixel_time import _get_native_pixel_time_offset
# satellite = "G16"
# sector = "F"
# scan_mode = "M3"
# resolution = "500"

 
# t_i = time.time()
# da = _get_native_pixel_time_offset(satellite=satellite,
#                                    sector=sector, 
#                                    scan_mode=scan_mode, 
#                                    resolution=resolution)

# t_f = time.time()
# t_f-t_i

    
# da = scn['C01']
# attrs = da.attrs
# satellite = attrs['platform_name']
# scan_mode = attrs['scan_mode']
# sector = attrs['scene_abbr'][0]
# resolution = attrs['resolution']
# start_time = attrs['start_time']  
# end_time = attrs['end_time']  
 
# from goes_api.abi_pixel_time import get_lut_filepath, get_pixel_time_offset
# fpath = get_lut_filepath(satellite, sector, scan_mode, resolution)
# ds = get_pixel_time_offset(satellite, sector, scan_mode, resolution)