#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 20 16:45:14 2022

@author: ghiggi
"""
import numpy as np 
from goes_api.info import get_key_from_filepaths, group_files


def _ensure_fpaths_list(func):
    """Decorator to ensure that the input to func is a list."""
    def inner(fpaths, *args, **kwargs): 
        if not isinstance(fpaths, (dict, list, str, np.ndarray)):
            raise ValueError("Expecting a file paths list or dictionary.")
        # If dictionary, convert to list 
        if isinstance(fpaths, dict):
            fpaths = list(fpaths.values())
            fpaths = [item for sublist in fpaths for item in sublist] 
        elif isinstance(fpaths, str):
            fpaths = [fpaths]
        elif isinstance(fpaths, np.ndarray): 
            fpaths = fpaths.tolist()
        result = func(fpaths, *args, **kwargs)
        return result 
    return inner


####--------------------------------------------------------------------------.
#### Operational checking tools 

@_ensure_fpaths_list
def ensure_operational_data(fpaths):
    """Check that the GOES files comes from the GOES Operational system Real-time (OR) environment."""
    # Check list of filepaths
    list_se = np.array(get_key_from_filepaths(fpaths, "system_environment"), dtype=str)
    # If not "OR" environment, raise an error 
    unvalid_idx = np.where(list_se != "OR")[0]
    if len(unvalid_idx) != 0:
        unvalid_fpaths = np.array(fpaths)[unvalid_idx] 
        raise ValueError(f"The required files does not come from the GOES operational system real-time environment. Unvalid files: {unvalid_fpaths}")


@_ensure_fpaths_list
def ensure_data_availability(fpaths, sensor=None, product=None, start_time=None, end_time=None):
    """Raise error if no data are available."""
    if len(fpaths) == 0:
        if sensor is None or product is None or start_time is None or end_time is None:
            raise ValueError("No data available.")
        else:
            raise ValueError(f"The {product} data between {start_time} and {end_time} are not available.")


@_ensure_fpaths_list  
def ensure_fixed_scan_mode(fpaths): 
    """Check fixed scan mode for ABI products."""
    scan_modes = get_key_from_filepaths(fpaths, "scan_mode")
    scan_modes = np.unique(scan_modes)
    if len(scan_modes) != 1:
        start_time = min(np.unique(get_key_from_filepaths(fpaths, "start_time")))
        end_time = max(np.unique(get_key_from_filepaths(fpaths, "end_time")))
        msg =  f"Multiple scan modes ({scan_modes}) occur between {start_time} and {end_time} !"
        print(msg)
        raise ValueError(msg)
        

def _raise_missing_data_error(missing_intervals, product): 
    error_message = f"Missing {product} data between:\n"
    for start_tt, end_tt in missing_intervals:
        error_message += f"[{start_tt} - {end_tt}]\n"
    raise ValueError(error_message)
    
    
def _ensure_not_missing_abi_acquisitions(fpaths, sector, product=''):
    """Check that there are not missing ABI acquisitions. 
    
    It take into account of the ABI scan modes.
    """
    from goes_api.listing import ABI_INTERVAL
    abi_interval = ABI_INTERVAL[sector].copy() # in minutes
    file_start_times = np.unique(get_key_from_filepaths(fpaths, "start_time"))
    file_end_times = np.unique(get_key_from_filepaths(fpaths, "end_time"))
    file_scan_modes = get_key_from_filepaths(fpaths, "scan_mode")
    
    missing_intervals = [] 
    for i in range(len(file_start_times) - 1):
       scan_mode = file_scan_modes[i]
       expected_interval = abi_interval[scan_mode]
       observed_interval = file_start_times[i+1] - file_start_times[i]
       if observed_interval.total_seconds()/60 > expected_interval:
           missing_intervals.append((file_end_times[i], file_start_times[i+1]))

    if missing_intervals:
        _raise_missing_data_error(missing_intervals, product)
      
    
def _ensure_not_missing_acquisitions(fpaths, product=''):
    """Check that there are not missing acquisitions. 
    
    It takes the smallest interval as the maximum allowed interval.
    """
    file_start_times = np.unique(get_key_from_filepaths(fpaths, "start_time"))
    file_end_times = np.unique(get_key_from_filepaths(fpaths, "end_time"))
    # Compute time intervals and infer minimum dt allowed
    timedeltas = np.diff(file_start_times).tolist()
    timedeltas = [dt.total_seconds() for dt in timedeltas]
    min_dt = np.min(timedeltas)
    missing_intervals = [] 
    for i in range(len(file_start_times) - 1):
       if timedeltas[i] > min_dt:
           missing_intervals.append((file_end_times[i], file_start_times[i+1]))
    if missing_intervals:
        if missing_intervals:
            _raise_missing_data_error(missing_intervals, product)
                   
       
def ensure_time_period_is_covered(fpaths, start_time, end_time, product=''):
    """Ensure the time period between the specified start_time and end_time is covered.
    
    It also check that there are not missing acquisitions.
    """
    # Check if there is at least 1 file
    if len(fpaths) == 0: 
        raise ValueError(f"Any {product} data available along the entire [{start_time}, {end_time}] period.")
    # Retrieve product and sensor infos (assuming single one)
    product = get_key_from_filepaths(fpaths[0], "product")[0]
    sensor = get_key_from_filepaths(fpaths[0], "sensor")[0]
    if sensor == "ABI": 
        sector = get_key_from_filepaths(fpaths[0], "sector")[0]
    # Retrieve available file start_time and end_time 
    # - ABI: minutes resolution required, GLM: seconds resolutions required (TODO !)
    file_start_times = np.unique(get_key_from_filepaths(fpaths, "start_time"))
    file_end_times = np.unique(get_key_from_filepaths(fpaths, "end_time"))
    #-----------------------------------------------
    # Check time period extremities are covered 
    data_start_time = np.min(file_start_times)
    data_end_time = np.max(file_end_times)
    if data_start_time > start_time:
        raise ValueError(f"The {product} data between {start_time} and {data_start_time} are not available.")
    if data_end_time < end_time: 
        raise ValueError(f"The {product} data between {data_end_time} and {end_time} are not available.")
     #-----------------------------------------------
    # If only 1 filepath, do not do further checks 
    if len(fpaths) == 1: 
        return None 
    #-----------------------------------------------
    # Check no missing acquisitions along the time period 
    if sensor == "ABI": 
        _ensure_not_missing_abi_acquisitions(fpaths, sector=sector, product=product)    
    else: 
        _ensure_not_missing_acquisitions(fpaths, product=product) 
    #-----------------------------------------------
    return None 


@_ensure_fpaths_list        
def ensure_all_files(fpaths, product=''):
    """Ensure same number of files for each timestep.
    
    It cannot catch cases when a file (i.e. ABI band) is missing at every timestep.
    """   
    # Retrieve fpath_dict
    fpaths_dict = group_files(fpaths, key="start_time")
    # Count number of files at each timestep 
    n_files = [len(fpaths)  for timestep, fpaths in fpaths_dict.items()]
    # If not constant number, raise error 
    if len(np.unique(n_files)) != 1: 
        raise ValueError(f"Missing {product} files across some timesteps.")
    return None  


def ensure_fpaths_validity(fpaths, sensor, start_time, end_time, product):
    # - Ensure that the file comes from the GOES Operational system Real-time (OR) environment
    ensure_operational_data(fpaths)
    # - Ensure data availability (there are some data)
    ensure_data_availability(fpaths, sensor=sensor, start_time=start_time, end_time=end_time, product=product)
    # - Ensure fixed scan mode (for ABI)
    ensure_fixed_scan_mode(fpaths)
    # - Ensure time period covered 
    ensure_time_period_is_covered(fpaths, start_time=start_time, end_time=end_time,product=product)
    # - Ensure same number of files per timestep (i.e. for Rad)
    # --> TODO: for ABI L1 and L2 Rad --> check 16 bands (or based on filter_params ... )
    ensure_all_files(fpaths, product=product)
