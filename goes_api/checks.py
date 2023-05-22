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
"""Define functions checking goes_api inputs."""

import os
import datetime
import numpy as np
from goes_api.alias import PROTOCOLS, _satellites, _sectors, _channels


def _check_protocol(protocol):
    """Check protocol validity."""
    if protocol is not None:
        if not isinstance(protocol, str):
            raise TypeError("`protocol` must be a string.")
        if protocol not in PROTOCOLS:
            raise ValueError(f"Valid `protocol` are {PROTOCOLS}.")
        if protocol == "local":
            protocol = "file"  # for fsspec LocalFS compatibility
    return protocol


def _check_download_protocol(protocol):
    """ "Check protocol validity for download."""
    if protocol not in ["gcs", "s3"]:
        raise ValueError("Please specify either 'gcs' or 's3' protocol for download.")


def _check_base_dir(base_dir):
    """Check base_dir validity."""
    if base_dir is not None:
        if not isinstance(base_dir, str):
            raise TypeError("`base_dir` must be a string.")
        if not os.path.exists(base_dir):
            raise OSError(f"`base_dir` {base_dir} does not exist.")
        if not os.path.isdir(base_dir):
            raise OSError(f"`base_dir` {base_dir} is not a directory.")
    return base_dir


def _check_satellite(satellite):
    """Check satellite validity."""
    if not isinstance(satellite, str):
        raise TypeError("`satellite` must be a string.")
    # Retrieve satellite key accounting for possible aliases
    satellite_key = None
    for key, possible_values in _satellites.items():
        if satellite.upper() in possible_values:
            satellite_key = key
            break
    if satellite_key is None:
        valid_satellite_key = list(_satellites.keys())
        raise ValueError(f"Available satellite: {valid_satellite_key}")
    return satellite_key


def _check_sector(sector, sensor, product=None):
    """Check sector validity."""
    from goes_api.info import available_sectors
    
    if sector is None: 
        if sensor == "ABI":
            raise ValueError("If sensor='ABI', `sector` must be specified!")
        return sector 
    if sector is not None and sensor != "ABI": 
        raise ValueError("`sector`must be specified only for sensor='ABI'.")
    if not isinstance(sector, str):
        raise TypeError("`sector` must be a string.")
    # Retrieve sector key accounting for possible aliases
    sector_key = None
    for key, possible_values in _sectors.items():
        if sector.upper() in possible_values:
            sector_key = key
            break
    # Raise error if provided unvalid sector key
    if sector_key is None:
        valid_sector_keys = list(_sectors.keys())
        raise ValueError(f"Available satellite: {valid_sector_keys}")
    # Check the sector is valid for a given product (if specified)
    valid_sectors = available_sectors(product=product)
    if product is not None:
        if sector_key not in valid_sectors:
            raise ValueError(
                f"Valid sectors for product {product} are {valid_sectors}."
            )
    return sector_key


def _check_sensor(sensor):
    """Check sensor validity."""
    from goes_api.info import available_sensors
    
    if not isinstance(sensor, str):
        raise TypeError("`sensor` must be a string.")
    valid_sensors = available_sensors()
    sensor = sensor.upper()
    if sensor not in valid_sensors:
        raise ValueError(f"Available sensors: {valid_sensors}")
    return sensor


def _check_sensors(sensors):
    """Check sensors validity."""
    if isinstance(sensors, str):
        sensors = [sensors]
    sensors = [_check_sensor(sensor) for sensor in sensors]
    return sensors


def _check_product_level(product_level, product=None):
    """Check product_level validity."""
    from goes_api.info import get_dict_product_level_products
    
    if not isinstance(product_level, str):
        raise TypeError("`product_level` must be a string.")
    product_level = product_level.capitalize()
    if product_level not in ["L1b", "L2"]:
        raise ValueError("Available product levels are ['L1b', 'L2'].")
    if product is not None:
        if product not in get_dict_product_level_products()[product_level]:
            raise ValueError(
                f"`product_level` '{product_level}' does not include product '{product}'."
            )
    return product_level


def _check_product_levels(product_levels):
    """Check product_levels validity."""
    if isinstance(product_levels, str):
        product_levels = [product_levels]
    product_levels = [
        _check_product_level(product_level) for product_level in product_levels
    ]
    return product_levels


def _check_product(product, sensor=None, product_level=None):
    """Check product validity."""
    from goes_api.info import available_products
    
    if not isinstance(product, str):
        raise TypeError("`product` must be a string.")
    valid_products = available_products(sensors=sensor, product_levels=product_level)
    # Retrieve product by accounting for possible aliases (upper/lower case)
    product_key = None
    for possible_values in valid_products:
        if possible_values.upper() == product.upper():
            product_key = possible_values
            break
    if product_key is None:
        if sensor is None and product_level is None:
            raise ValueError(f"Available products: {valid_products}")
        else:
            sensor = "" if sensor is None else sensor
            product_level = "" if product_level is None else product_level
            raise ValueError(
                f"Available {product_level} products for {sensor}: {valid_products}"
            )
    return product_key


def _check_time(time):
    """Check time validity."""
    if not isinstance(time, (datetime.datetime, datetime.date, np.datetime64, str)):
        raise TypeError(
            "Specify time with datetime.datetime objects or a "
            "string of format 'YYYY-MM-DD hh:mm:ss'."
        )
    # If np.datetime, convert to datetime.datetime
    if isinstance(time, np.datetime64):
        time = time.astype('datetime64[s]').tolist()
    # If datetime.date, convert to datetime.datetime
    if not isinstance(time, (datetime.datetime, str)):
        time = datetime.datetime(time.year, time.month, time.day, 0, 0, 0)
    if isinstance(time, str):
        try:
            time = datetime.datetime.fromisoformat(time)
        except ValueError:
            raise ValueError("The time string must have format 'YYYY-MM-DD hh:mm:ss'")
            
    # Set resolution to minutes
    # TODO: CONSIDER POSSIBLE MESOSCALE AT 30 SECS
    time = time.replace(microsecond=0, second=0)
    return time


def check_date(date):
    """Check date validity."""
    if not isinstance(date, (datetime.date, datetime.datetime)):
        raise ValueError("date must be a datetime object")
    if isinstance(date, datetime.datetime):
        date = date.date()
    return date


def _check_start_end_time(start_time, end_time):
    """Check start_time and end_time validity."""
    # Format input
    start_time = _check_time(start_time)
    end_time = _check_time(end_time)
    # Check start_time and end_time are chronological
    if start_time > end_time:
        raise ValueError("Provide start_time occuring before of end_time")
    # Check start_time is in the past
    if start_time > datetime.datetime.utcnow():
        raise ValueError("Provide a start_time occuring in the past.")
        
    # end_time must not be checked if wanting to search on latest file available ! 
    # if end_time > datetime.datetime.utcnow():
    #     raise ValueError("Provide a end_time occuring in the past.")
    return (start_time, end_time)


def _check_year_month(year, month):
    """Check year month validity."""
    # TODO: check before current date and after xxx for specific satellites
    _check_month(month)
    _check_year(year) 
    return year, month 
 
    
def _check_month(month):
    """Check month value."""
    if not isinstance(month, int):
        raise TypeError("'month' must be provided as an integer.")
    if month < 1 or month > 12: 
        raise ValueError("'month' value must be between 1 and 12.")


def _check_year(year):
    """Check year value."""
    if not isinstance(year, int):
        raise TypeError("'year' must be provided as an integer.")
    current_year = datetime.datetime.now().year 
    if year > current_year:
        raise ValueError("'year' must not exceed current year.")
        

def _check_channel(channel):
    """Check channel validity."""
    if not isinstance(channel, str):
        raise TypeError("`channel` must be a string.")
    # Check channel follow standard name
    channel = channel.upper()
    if channel in list(_channels.keys()):
        return channel
    # Retrieve channel key accounting for possible aliases
    else:
        channel_key = None
        for key, possible_values in _channels.items():
            if channel.upper() in possible_values:
                channel_key = key
                break
        if channel_key is None:
            valid_channels_key = list(_channels.keys())
            raise ValueError(f"Available channels: {valid_channels_key}")
        return channel_key


def _check_channels(channels=None, sensor=None):
    """Check channels validity."""
    if channels is None:
        return channels
    if sensor is not None:
        if sensor != "ABI":
            raise ValueError("`sensor` must be 'ABI' if the channels are specified!")
    if isinstance(channels, str):
        channels = [channels]
    channels = [_check_channel(channel) for channel in channels]
    return channels


def _check_scan_mode(scan_mode):
    """Check scan_mode validity."""
    from goes_api.info import available_scan_modes
    
    if not isinstance(scan_mode, str):
        raise TypeError("`scan_mode` must be a string.")
    # Check channel follow standard name
    scan_mode = scan_mode.upper()
    valid_scan_modes = available_scan_modes()
    if scan_mode in valid_scan_modes:
        return scan_mode
    else:
        raise ValueError(f"Available `scan_mode`: {valid_scan_modes}")


def _check_scan_modes(scan_modes=None, sensor=None):
    """Check scan_modes validity."""
    if scan_modes is None:
        return scan_modes
    if sensor is not None:
        if sensor != "ABI":
            raise ValueError("`sensor` must be 'ABI' if the scan_mode is specified!")
    if isinstance(scan_modes, str):
        scan_modes = [scan_modes]
    scan_modes = [_check_scan_mode(scan_mode) for scan_mode in scan_modes]
    return scan_modes


def _check_scene_abbr(scene_abbr, sensor=None, sector=None):
    """Check ABI mesoscale sector scene_abbr validity."""
    if scene_abbr is None:
        return scene_abbr
    if sensor is not None:
        if sensor != "ABI":
            raise ValueError("`sensor` must be 'ABI' if the scene_abbr is specified!")
    if sector is not None:
        if sector != "M":
            raise ValueError("`scene_abbr` must be specified only if sector=='M' !")
    if not isinstance(scene_abbr, (str, list)):
        raise TypeError("Specify `scene_abbr` as string or list.")
    if isinstance(scene_abbr, list):
        if len(scene_abbr) == 1:
            scene_abbr = scene_abbr[0]
        else:
            return None  # set to None assuming ['M1' and 'M2']
    if scene_abbr not in ["M1", "M2"]:
        raise ValueError("Valid `scene_abbr` values are 'M1' or 'M2'.")
    return scene_abbr


def _check_system_environment(fpaths, value="OR"):
    """
    The GOES system environment defines whether the data in the file 
    are real-time, test, playback, or simulated data.
    
    Possible values: 
    “OR” = operational system real-time data
    “OT” = operational system test data
    “IR” = test system real-time data
    “IT” = test system test data
    “IP” = test system playback data
    “IS” = test system simulated data
    """
    valid_values = ["OR", "OT", "IR", "IT", "IP", "IS"]
    if value not in valid_values:
        raise ValueError(f"{value} is not a valid GOES system environment. Valid values are {valid_values}.")
        

def _check_filter_parameters(filter_parameters, sensor, sector):
    """Check filter parameters validity.

    It ensures that scan_modes, channels and scene_abbr are valid lists (or None).
    """
    if not isinstance(filter_parameters, dict):
        raise TypeError("filter_parameters must be a dictionary.")
    scan_modes = filter_parameters.get("scan_modes")
    channels = filter_parameters.get("channels")
    scene_abbr = filter_parameters.get("scene_abbr")
    if scan_modes:
        filter_parameters["scan_modes"] = _check_scan_modes(scan_modes)
    if channels:
        filter_parameters["channels"] = _check_channels(channels, sensor=sensor)
    if scene_abbr:
        filter_parameters["scene_abbr"] = _check_scene_abbr(
            scene_abbr, sensor=sensor, sector=sector
        )
    return filter_parameters


def _check_group_by_key(group_by_key):
    """Check group_by_key validity."""
    from goes_api.info import available_group_keys
    
    if not isinstance(group_by_key, (str, type(None))):
        raise TypeError("`group_by_key`must be a string or None.")
    if group_by_key is not None:
        valid_group_by_key = available_group_keys()
        if group_by_key not in valid_group_by_key:
            raise ValueError(
                f"{group_by_key} is not a valid group_by_key. "
                f"Valid group_by_key are {valid_group_by_key}."
            )
    return group_by_key


def _check_connection_type(connection_type, protocol):
    """Check cloud bucket connection_type validity."""
    if not isinstance(connection_type, (str, type(None))):
        raise TypeError("`connection_type` must be a string (or None).")
    if protocol is None:
        connection_type = None
    if protocol in ["file", "local"]:
        connection_type = None  # set default
    if protocol in ["gcs", "s3"]:
        # Set default connection type
        if connection_type is None:
            connection_type = "bucket"  # set default
        valid_connection_type = ["bucket", "https", "nc_bytes"]
        if connection_type not in valid_connection_type:
            raise ValueError(f"Valid `connection_type` are {valid_connection_type}.")
    return connection_type


def _check_unique_scan_mode(fpath_dict, sensor):
    """Check files have unique scan_mode validity."""
    from goes_api.info import get_key_from_filepaths
    
    # TODO: raise information when it changes
    if sensor == "ABI":
        list_datetime = list(fpath_dict.keys())
        fpaths_examplars = [fpath_dict[tt][0] for tt in list_datetime]
        list_scan_modes = get_key_from_filepaths(fpaths_examplars, key="scan_mode")
        list_scan_modes = np.unique(list_scan_modes).tolist()
        if len(list_scan_modes) != 1:
            raise ValueError(
                f"There is a mixture of the following scan_mode: {list_scan_modes}."
            )


def _check_interval_regularity(list_datetime):
    """Check regularity of a list of timesteps."""
    # TODO: raise info when missing between ... and ...
    if len(list_datetime) < 2:
        return None
    list_datetime = sorted(list_datetime)
    list_timedelta = np.diff(list_datetime)
    list_unique_timedelta = np.unique(list_timedelta)
    if len(list_unique_timedelta) != 1:
        raise ValueError("The time interval is not regular!")
