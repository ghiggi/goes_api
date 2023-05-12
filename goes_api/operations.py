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
            raise ValueError(f"No {sensor} {product} data available between {start_time} and {end_time} !")


@_ensure_fpaths_list        
def ensure_regular_timesteps(fpaths, timedelta=None):
    """Ensure files are regularly separated ... by "timedelta" seconds (if not None)."""
    # Retrieve available timesteps
    timesteps = np.unique(get_key_from_filepaths(fpaths, "start_time"))
    # Check enough timesteps
    if len(timesteps) == 0: 
        raise ValueError("No timesteps available !")
    if len(timesteps) == 1: 
        raise ValueError("Only one timestep available !")
    # Compute unique time deltas
    timedeltas = [dt.seconds for dt in list(np.diff(timesteps))]
    timedeltas = np.unique(timedeltas)
    # Check unique timedelta
    if len(timedeltas) != 1:
        raise ValueError(f"No unique time interval between GEO imagery. Time intervals are: {timedeltas} !")
    # Check if unique timedelta meets the expectations
    if timedelta is not None: 
        if timedeltas != timedelta:
            raise ValueError(f"The time interval between GEO imagery is not {timedelta} !")


@_ensure_fpaths_list        
def ensure_time_period_is_covered(fpaths, start_time, end_time, product=''):
    """Ensure the time period between the specified start_time and end_time is covered."""
    # Retrieve available file start_time and end_time
    file_start_times = np.unique(get_key_from_filepaths(fpaths, "start_time"))
    file_end_times = np.unique(get_key_from_filepaths(fpaths, "end_time"))
    # Retrieve effective data_start_time and end_time
    data_start_time = np.min(file_start_times)
    data_end_time = np.max(file_end_times)
    if data_start_time > start_time:
        raise ValueError(f"{product} Data between {start_time} and {data_start_time} are not available.")
    if data_end_time < end_time: 
        raise ValueError(f"{product} Data between {data_end_time} and {end_time} are not available.")


@_ensure_fpaths_list        
def ensure_all_files(fpaths, product=''):
    """Ensure same number of files for each timestep.
    
    It cannot catch cases when a file is missing at every timestep.
    """   
    # Retrieve fpath_dict
    fpaths_dict = group_files(fpaths, key="start_time")
    # Count number of files at each timestep 
    n_files = [len(fpaths)  for timestep, fpaths in fpaths_dict.items()]
    # If not constant number, raise error 
    if len(np.unique(n_files)) != 1: 
        raise ValueError(f"Missing {product} files across some timesteps.")
    return None  

