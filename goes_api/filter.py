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
"""Define filtering filepaths functions."""

from goes_api.info import _get_info_from_filepath
from goes_api.checks import (
     _check_channels,
     _check_scene_abbr,
     _check_start_end_time,
     _check_product_level,
     _check_sensor,
     _check_scan_modes,
)

# TODO: enable also filtering by product !


def _ensure_list_if_str(x): 
    if x is None: 
        return x
    if isinstance(x, str): 
        return [x] 
    if isinstance(x, list):
        return x 
    else: 
        raise TypeError("Not expected.")
    
        
def _filter_file(
    fpath,
    sensor=None,
    product_level=None, 
    start_time=None,
    end_time=None,
    scan_modes=None,
    channels=None,
    scene_abbr=None,
):
    """Utility function to filter a filepath based on optional filter_parameters."""
    # start_time and end_time must be a datetime object
    sensor = _ensure_list_if_str(sensor)
    product_level = _ensure_list_if_str(product_level)
    scan_modes = _ensure_list_if_str(scan_modes)
    channels = _ensure_list_if_str(channels)
    scene_abbr = _ensure_list_if_str(scene_abbr)
    
    # Get info from filepath
    info_dict = _get_info_from_filepath(fpath)

    # Filter by sensor 
    if sensor is not None:
       
        file_sensor = info_dict.get("sensor")
        if file_sensor is not None:
            if file_sensor not in sensor: 
                return None
    
    # Filter by product level 
    if product_level is not None:
        file_product_level = info_dict.get("product_level")
        if file_product_level is not None:
            if file_product_level not in product_level: 
                return None
            
    # Filter by channels
    if channels is not None:
        file_channel = info_dict.get("channel")
        if file_channel is not None:
            if file_channel not in channels:
                return None

    # Filter by scan mode
    if scan_modes is not None:
        file_scan_mode = info_dict.get("scan_mode")
        if file_scan_mode is not None:
            if file_scan_mode not in scan_modes:
                return None

    # Filter by scene_abbr
    if scene_abbr is not None:
        file_scene_abbr = info_dict.get("scene_abbr")
        if file_scene_abbr is not None:
            if file_scene_abbr not in scene_abbr:
                return None
    
    # Filter by start_time
    if start_time is not None:
        # If the file ends before start_time, do not select
        # - Do not use <= because mesoscale data can have start_time=end_time at min resolution
        file_end_time = info_dict.get("end_time")
        if file_end_time < start_time: 
            return None
        # This would exclude a file with start_time within the file
        # if file_start_time < start_time:
        #     return None

    # Filter by end_time
    if end_time is not None:
        file_start_time = info_dict.get("start_time")
        # If the file starts after end_time, do not select
        if file_start_time > end_time:
            return None
        # This would exclude a file with end_time within the file
        # if file_end_time > end_time:
        #     return None
    return fpath


def _filter_files(
    fpaths,
    sensor=None,
    product_level=None,
    start_time=None,
    end_time=None,
    scan_modes=None,
    channels=None,
    scene_abbr=None,
):
    """Utility function to select filepaths matching optional filter_parameters."""
    if isinstance(fpaths, str):
        fpaths = [fpaths]
    fpaths = [
        _filter_file(
            fpath,
            sensor=sensor, 
            product_level=product_level, 
            start_time=start_time,
            end_time=end_time,
            scan_modes=scan_modes,
            channels=channels,
            scene_abbr=scene_abbr,
        )
        for fpath in fpaths
    ]
    fpaths = [fpath for fpath in fpaths if fpath is not None]
    return fpaths


def filter_files(
    fpaths,
    sensor=None,
    product_level=None, 
    start_time=None,
    end_time=None,
    scan_modes=None,
    scene_abbr=None,
    channels=None,
):
    """
    Filter files by optional parameters.

    The optional parameters can also be defined within a `filter_parameters`
    dictionary which is then passed to `find_files` or `download_files` functions.

    Parameters
    ----------
    fpaths : list
        List of filepaths.
    sensor : str or list, optional
        Satellite sensor(s).
        See `goes_api.available_sensors()` for available sensors.
        The default is None (no filtering by sensor).
    product_level : str or list, optional
        Product level.
        See `goes_api.available_product_levels()` for available product levels.
        The default is None (no filtering by product_level).
    start_time : datetime.datetime, optional
        Time defining interval start.
        The default is None (no filtering by start_time).
    end_time : datetime.datetime, optional
        Time defining interval end.
        The default is None (no filtering by end_time).
    scan_modes : list, optional
        List of ABI scan modes to select.
        See `goes_api.available_scan_modes()` for available scan modes.
        The default is None (no filtering by scan_modes).
    scene_abbr : str or list, optional
        String specifying selection of mesoscale scan region.
        Either M1 or M2.
        The default is None (no filtering by mesoscale scan region).
    channels : list, optional
        List of ABI channels to select.
        See `goes_api.available_channels()` for available ABI channels.
        The default is None (no filtering by channels).

    """
    sensor = _check_sensor(sensor)
    product_level = _check_product_level(product_level, product=None)
    scan_modes = _check_scan_modes(scan_modes)
    channels = _check_channels(channels, sensor=sensor)
    scene_abbr = _check_scene_abbr(scene_abbr, sensor=sensor)
    start_time, end_time = _check_start_end_time(start_time, end_time)
    fpaths = _filter_files(
        fpaths=fpaths,
        sensor=sensor,
        product_level=product_level,
        start_time=start_time,
        end_time=end_time,
        scan_modes=scan_modes,
        channels=channels,
        scene_abbr=scene_abbr,
    )
    return fpaths