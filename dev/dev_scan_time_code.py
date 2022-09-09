#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 17:09:02 2022

@author: ghiggi
"""
import numpy as np 

def get_scan_duration():
    """
    From Table 2. of Kalluri et al., 2018. 
    From Photons to Pixels: Processing Data from the Advanced Baseline Imager.
    Remote Sensing, MDPI 
    """
    scan_duration = [
     6.750,
     8.710,
     10.104,
     11.172,
     12.011,
     12.672,
     13.185,
     13.568,
     13.834,
     13.991,
     14.041,
     14.041,
     13.991,
     13.834,
     13.568,
     13.185,
     12.672,
     12.011,
     11.172,
     10.104,
     8.710,
     6.750
    ]
    scan_duration = np.array(scan_duration)
    return scan_duration

def get_scan_time_offsets(satellite, scan_mode):
    if satellite == "G16": 
        if scan_mode == "M6":
            return get_G16_M6() 
        elif scan_mode == "M4":
            return get_scan_mode_M4()
        elif scan_mode == "M3": 
            return get_G16_M3()
        else: 
            raise ValueError("Invalid `scan_mode`")
    elif satellite == "G17":
        if scan_mode == "M6":
            return get_G17_M6() 
        elif scan_mode == "M4":
            return get_scan_mode_M4()
        elif scan_mode == "M3": 
            return get_G17_M3_Cooling()
        else: 
            raise ValueError("Invalid `scan_mode`")
    else: 
        raise ValueError("Invalid `satellite`")

def get_G16_M6(): 
    # Mode 6:  09:31s 
    "https://www.ospo.noaa.gov/Operations/GOES/16/GOES-16%20Scan%20Mode%206.html"
    
    start_time_offset = [
        0 + 19.8,
        30 + 3.8,
        30 + 12.9,
        60 + 4.5,
        90 + 3,
        120 + 2.75,
        150 + 2.5,
        180 + 2.3,
        210 + 2.1,
        240 + 2,
        270 + 2, 
        300 + 2,
        330 + 2,
        360 + 2.1,
        390 + 2.3,
        420 + 2.5,
        450 + 2.8,
        480 + 3.2,
        510 + 3.6, 
        540 + 4,
        570 + 4.8,
        570 + 13.9,
    ]
    start_time_offset = np.array(start_time_offset)
    end_time_offset = start_time_offset + get_scan_duration()
    return start_time_offset, end_time_offset

def get_G17_M6(): 
    # Mode 6:  09:31s 
    "https://www.ospo.noaa.gov/Operations/GOES/west/GOES-17%20Scan%20Mode%206.html"
    start_time_offset = [
        30 + 1.4,
        30 + 8.6,
        30 + 18,
        60 + 5.1,
        90 + 2.2,
        120 + 2,
        150 + 1.8,
        180 + 1.5,
        210 + 1.4,
        240 + 1.3,
        270 + 1.3,
        300 + 1.3,
        330 + 1.3,
        360 + 1.4,
        390 + 1.6,
        420 + 1.8,
        450 + 2,
        480 + 2.3,
        510 + 2.8,
        540 + 3.2,
        540 + 14,
        570 + 1.55,     
    ]
    start_time_offset = np.array(start_time_offset)
    end_time_offset = start_time_offset + get_scan_duration()
    return start_time_offset, end_time_offset
    
def get_G17_M3_Cooling(): 
    "https://www.ospo.noaa.gov/Operations/GOES/west/Mode3G_Cooling_Timeline_G17.html"
    start_time_offset = [
        30 + 5.6,
        60 + 4.6,
        90 + 3.8,
        120 + 3.2,
        150 + 2.8,
        180 + 2.6,
        210 + 2.3,
        240 + 2.15,
        270 + 2,
        300 + 1.9,
        330 + 1.9,
        360 + 1.9,
        390 + 1.9,
        420 + 2,
        450 + 2.15,
        480 + 2.3,
        510 + 2.6,
        540 + 2.8,
        570 + 3.2,
        600 + 3.8,
        630 + 4.6,
        660 + 5.6,
    ]
    start_time_offset = np.array(start_time_offset)
    end_time_offset = start_time_offset + get_scan_duration()
    return start_time_offset, end_time_offset
 
def get_scan_mode_M4(): 
    "https://www.ospo.noaa.gov/Operations/GOES/west/Mode3G_Cooling_Timeline_G17.html"
    start_time_offset = [
        0 + 19.8, 
        0 + 27.1,
        30 + 6.4, 
        30 + 17, 
        30 + 28.8,
        60 + 11.4, 
        60 + 24.6, 
        90 + 12.3,
        90 + 26.4, 
        120 + 10.9,
        120 + 25.6,
        150 + 10.2, 
        150 + 24.9, 
        180 + 13.2,
        180 + 27.6,
        210 + 11.9,
        210 + 25.7, 
        240 + 9, 
        240 + 21.5,
        270 + 3.4, 
        270 + 14,
        270 + 23.2, 
    ]
    start_time_offset = np.array(start_time_offset)
    end_time_offset = start_time_offset + get_scan_duration()
    return start_time_offset, end_time_offset
        
        
             
def get_G16_M3(): 
    """A Closer look at the ABI  on the GOES-R."""
    # Every 15 minutes
    start_time_offset = [
        30 + 5.6,
        60 + 4.6,
        90 + 3.8,
        120 + 3.2,
        150 + 2.8,
        180 + 2.6,
        210 + 2.3,
        240 + 2.15,
        270 + 2,
        300 + 1.9,
        330 + 1.9,
        360 + 1.9,
        390 + 1.9,
        420 + 2,
        450 + 2.15,
        480 + 2.3,
        510 + 2.6,
        540 + 2.8,
        570 + 3.2,
        600 + 3.8,
        630 + 4.6,
        660 + 5.6,       
    ]
    start_time_offset = np.array(start_time_offset)
    end_time_offset = start_time_offset + get_scan_duration()
    return start_time_offset, end_time_offset
        
def get_valid_column_indices(disc_mask, row_index):
    height, width = disc_mask.shape 
    col_start = np.where(np.isfinite(disc_mask[row_index,:]))[0][0]
    col_end = width - 1 - np.where(np.isfinite(disc_mask[row_index,::-1]))[0][0]
    return col_start, col_end

def get_time_offset_array(disc_mask, satellite, scan_mode):
    # Retrieve swath row start indices  
    height, width = disc_mask.shape 
    row_start_indices = np.linspace(0, width, 23)[:-1].astype(int)
    row_start_indices[0] = np.where(~np.all(np.isnan(disc_mask[0:row_start_indices[1],:]), axis=1))[0][0]
    valid_last_index = row_start_indices[-1] + np.where(~np.all(np.isnan(disc_mask[row_start_indices[-1]:,:]), axis=1))[0][-1]
    row_start_indices = np.append(row_start_indices, valid_last_index)
    # Retrieve scan slices 
    row_scan_slices = [slice(row_start_indices[i],row_start_indices[i+1]) for i in range(len(row_start_indices)-1)]
     
    # Retrieve scan time offset bounds   
    start_time_offset, end_time_offset = get_scan_time_offsets(satellite, scan_mode)
    
    # Retrieve array time offset 
    time_offset_arr = np.zeros((height, width))*np.nan
    for i, scan_slice in enumerate(row_scan_slices): 
        row_scan_start = scan_slice.start
        row_scan_end =  scan_slice.stop - 1 
        col_start1, col_end1 = get_valid_column_indices(disc_mask, row_scan_start)
        col_start2, col_end2 = get_valid_column_indices(disc_mask, row_scan_end)
        col_start = min(col_start1, col_start2)
        col_end = max(col_end1, col_end2)
        n_pixels = col_end - col_start + 1
        time_offset_scan = np.linspace(start_time_offset[i], end_time_offset[i], n_pixels)
        time_offset_arr[row_scan_start:row_scan_end+1, col_start:col_end+1] = time_offset_scan
        
    # Remove offset to starting the first scan 
    time_offset_arr = time_offset_arr - start_time_offset[0]
    
    # Return array
    time_offset_arr = time_offset_arr*disc_mask
    return time_offset_arr         
        
        

        
        
        
        
        