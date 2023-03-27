#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 15:10:06 2022

@author: ghiggi
"""
import os
import xarray as xr 
from goes_api.abi_pixel_time import _get_native_pixel_time_offset

# Set directory 
dst_dir = "/home/ghiggi/Python_Packages/goes_api/goes_api/data/ABI_Pixel_TimeOffset"
os.makedirs(dst_dir, exist_ok=True)

#-----------------------------------------------------------------------------.
# Define attributes and chunksize
attrs = {"comment": "Pixel offset in s from ABI file start_time.",
         "source": "https://www.star.nesdis.noaa.gov/GOESCal/goes_tools.php"
        }

chunksize_dict = {"F": (226, 226),
                  "C": (250, 250), 
                  "M": (250, 250), 
                 }

#-----------------------------------------------------------------------------.
# Precompute pixel offsets
satellites = ["goes-16", "goes-17"]
scan_modes = ["M3","M4", "M6"]
sectors = ["F", "C", "M"]
resolutions = ["500", "1000", "2000"]

for satellite in satellites: 
    for scan_mode in scan_modes: 
        for sector in sectors: 
            for resolution in resolutions: 
                # Get pixel offset 
                try:
                    da = _get_native_pixel_time_offset(satellite=satellite,
                                                       sector=sector, 
                                                       scan_mode=scan_mode, 
                                                       resolution=resolution)
                except KeyError: 
                    continue 
                
                # Approximate to second precision 
                da.data = da.data.astype(int)/1000/1000/1000  
                
                # Conversion to Dataset 
                ds = da.to_dataset()
                ds.attrs = attrs 
                
                # Define encodings
                default_encoding = {"dtype": "uint16",
                                    "zlib": True, 
                                    "complevel": 1,
                                    "shuffle": False,
                                    "contiguous": False,
                                    "chunksizes": chunksize_dict[sector],
                                    "add_offset": -50, 
                                    "_FillValue": 65535, 
                                    }
                encoding = {} 
                for var in ds.data_vars:
                    encoding[var] = default_encoding
                    
                # Save to disk 
                fname = "_".join([satellite, sector, scan_mode, resolution]) + ".nc"
                fpath = os.path.join(dst_dir, fname)
                print("Writing:", fpath)
                ds.to_netcdf(fpath, encoding=encoding)