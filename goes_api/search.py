#!/usr/bin/env python3

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
"""Functions for searching files on local disk and cloud buckets."""

import datetime
import os

import numpy as np

from goes_api.checks import (
    _check_base_dir,
    _check_connection_type,
    _check_filter_parameters,
    _check_group_by_key,
    _check_product,
    _check_product_level,
    _check_protocol,
    _check_satellite,
    _check_sector,
    _check_sensor,
    _check_start_end_time,
    _check_time,
)
from goes_api.configs import get_goes_base_dir
from goes_api.filter import _filter_files
from goes_api.info import group_files
from goes_api.io import (
    _get_bucket_prefix,
    _get_product_dir,
    _get_time_dir_tree,
    _set_connection_type,
    get_filesystem,
)
from goes_api.operations import (
    ensure_all_files,
    # ensure_operational_data,
    # ensure_data_availability,
    # ensure_fixed_scan_mode,
    # ensure_time_period_is_covered,
    ensure_fpaths_validity,
)

####--------------------------------------------------------------------------.


def _get_acquisition_max_timedelta(sector):
    """Get reasonable timedelta based on ABI sector to find previous/next acquisition."""
    if sector == "M":
        dt = datetime.timedelta(minutes=1)
    elif sector == "C":
        dt = datetime.timedelta(minutes=5)
    elif sector == "F":
        dt = datetime.timedelta(minutes=15)  # to include all scan_mode options
    else:  # sector=None (all other sensors)  # TODO: might be improved ...
        dt = datetime.timedelta(minutes=15)
    return dt


def _enable_multiple_products(func):
    """Decorator to retrieve filepaths for multiple products."""

    def decorator(*args, **kwargs):
        # Single product case
        if isinstance(kwargs["product"], str):
            fpaths = func(*args, **kwargs)
            return fpaths

        # Multiproduct case
        if isinstance(kwargs["product"], list):
            products = kwargs["product"]
            group_by_key = kwargs.get("group_by_key", None)
            operational_checks = kwargs.get("operational_checks", True)
            list_fpaths = []
            for product in products:
                new_kwargs = kwargs.copy()
                new_kwargs["product"] = product
                new_kwargs["group_by_key"] = None
                fpaths = func(*args, **new_kwargs)
                list_fpaths.append(fpaths)
            # Flat the list
            fpaths = [item for sublist in list_fpaths for item in sublist]
            # Operational checks
            if operational_checks:
                ensure_all_files(fpaths)
            # Group files if asked
            if group_by_key is not None:
                fpaths = group_files(fpaths, key=group_by_key)
            return fpaths
        raise ValueError("Expecting 'product' to be a string or a list.")

    return decorator


@_enable_multiple_products
def find_files(
    satellite,
    sensor,
    product_level,
    product,
    start_time,
    end_time,
    sector=None,
    filter_parameters={},
    group_by_key=None,
    connection_type=None,
    base_dir=None,
    protocol="file",
    fs_args={},
    verbose=False,
    operational_checks=True,
):
    """
    Retrieve files from local or cloud bucket storage.

    If you are querying mesoscale domain data (sector=M), you might be
      interested to specify in the filter_parameters dictionary the
      key `scene_abbr` with values "M1" or "M2".

    Parameters
    ----------
    base_dir : str, optional
        This argument must be specified only if searching files on the local storage
        when protocol="file".
        It represents the path to the local directory where to search for GOES data.
        If protocol="file" and base_dir is None, base_dir is retrieved from
        the GOES-API config file.
        The default is None.
    protocol : str (optional)
        String specifying the location where to search for the data.
        If protocol="file", it searches on local storage (indicated by base_dir).
        Otherwise, protocol refers to a specific cloud bucket storage.
        Use `goes_api.available_protocols()` to check the available protocols.
        The default is "file".
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `goes_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `goes_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `goes_api.available_products()` for a list of available products.
    start_time : datetime.datetime
        The start (inclusive) time of the interval period for retrieving the filepaths.
    end_time : datetime.datetime
        The end (exclusive) time of the interval period for retrieving the filepaths.
    sector : str
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    group_by_key : str, optional
        Key by which to group the list of filepaths
        See `goes_api.available_group_keys()` for available grouping keys.
        If a key is provided, the function returns a dictionary with grouped filepaths.
        By default, no key is specified and the function returns a list of filepaths.
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `goes_api.available_connection_types` for implemented solutions.
    verbose : bool, optional
        If True, it print some information concerning the file search.
        The default is False.
    operational_checks: bool, optional
        If True, it checks that:
        1. the file comes from the GOES Operational system Real-time (OR) environment
        2. the scan mode is fixed (for ABI)
        4. the time period between start_time and end_time is fully covered,
            without missing acquisitions.
    """
    # Check inputs
    if protocol not in ["file", "local"] and base_dir is not None:
        raise ValueError("If protocol is not 'file' or 'local', base_dir must not be specified !")

    # Check for when searching on local storage
    if protocol in ["file", "local"]:
        # Get default local directory if base_dir = None
        base_dir = get_goes_base_dir(base_dir)
        # Set protocol and fs_args expected by fsspec
        protocol = "file"
        fs_args = {}

    # -------------------------------------------------------------------------.
    # Format inputs
    protocol = _check_protocol(protocol)
    base_dir = _check_base_dir(base_dir)
    connection_type = _check_connection_type(connection_type, protocol)
    satellite = _check_satellite(satellite)
    sensor = _check_sensor(sensor)
    product_level = _check_product_level(product_level, product=None)
    product = _check_product(product, sensor=sensor, product_level=product_level)
    sector = _check_sector(sector, product=product, sensor=sensor)
    start_time, end_time = _check_start_end_time(start_time, end_time)

    filter_parameters = _check_filter_parameters(filter_parameters, sensor, sector=sector)
    group_by_key = _check_group_by_key(group_by_key)

    # Add start_time and end_time to filter_parameters
    filter_parameters = filter_parameters.copy()
    filter_parameters["start_time"] = start_time
    filter_parameters["end_time"] = end_time

    # Get filesystem
    fs = get_filesystem(protocol=protocol, fs_args=fs_args)

    bucket_prefix = _get_bucket_prefix(protocol)

    # Get product dir
    product_dir = _get_product_dir(
        protocol=protocol,
        base_dir=base_dir,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
    )

    # Define time directories (YYYY/DOY/HH) where to search for data
    list_dir_tree = _get_time_dir_tree(start_time, end_time)

    # Define glob patterns
    list_glob_pattern = [os.path.join(product_dir, dir_tree, "*.nc*") for dir_tree in list_dir_tree]
    n_directories = len(list_glob_pattern)
    if verbose:
        print(f"Searching files across {n_directories} directories.")

    # Loop over each directory:
    # - TODO in parallel ?
    list_fpaths = []
    # glob_pattern = list_glob_pattern[0]
    for glob_pattern in list_glob_pattern:
        # Retrieve list of files
        fpaths = fs.glob(glob_pattern)
        # Add bucket prefix
        fpaths = [bucket_prefix + fpath for fpath in fpaths]
        # Filter files if necessary
        if len(filter_parameters) >= 1:
            fpaths = _filter_files(fpaths, sensor, product_level, **filter_parameters)
        list_fpaths += fpaths

    fpaths = list_fpaths

    # Perform checks for operational routines
    if operational_checks:
        ensure_fpaths_validity(
            fpaths,
            sensor=sensor,
            start_time=start_time,
            end_time=end_time,
            product=product,
        )

    # Group fpaths by key
    if group_by_key:
        fpaths = group_files(fpaths, key=group_by_key)

    # Parse fpaths for connection type
    fpaths = _set_connection_type(
        fpaths,
        satellite=satellite,
        protocol=protocol,
        connection_type=connection_type,
    )
    # Return fpaths
    return fpaths


def find_closest_start_time(
    time,
    satellite,
    sensor,
    product_level,
    product,
    sector=None,
    base_dir=None,
    protocol="file",
    fs_args={},
    filter_parameters={},
):
    """
    Retrieve files start_time closest to the specified time.

    Parameters
    ----------
    time : datetime.datetime
        The time for which you desire to know the closest file start_time.
    base_dir : str, optional
        This argument must be specified only if searching files on the local storage
        when protocol="file".
        It represents the path to the local directory where to search for GOES data.
        If protocol="file" and base_dir is None, base_dir is retrieved from
        the GOES-API config file.
        The default is None.
    protocol : str (optional)
        String specifying the location where to search for the data.
        If protocol="file", it searches on local storage (indicated by base_dir).
        Otherwise, protocol refers to a specific cloud bucket storage.
        Use `goes_api.available_protocols()` to check the available protocols.
        The default is "file".
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `goes_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `goes_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `goes_api.available_products()` for a list of available products.
    sector : str
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    filter_parameters: dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    """
    # Set time precision to minutes
    time = _check_time(time)
    time = time.replace(microsecond=0, second=0)
    # Retrieve timedelta conditioned to sector (for ABI)
    timedelta = _get_acquisition_max_timedelta(sector)
    # Define start_time and end_time
    start_time = time - timedelta
    end_time = time + timedelta
    # Retrieve files
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        operational_checks=False,
        verbose=False,
    )
    # Select start_time closest to time
    list_datetime = sorted(list(fpath_dict.keys()))
    if len(list_datetime) == 0:
        dt_str = int(timedelta.seconds / 60)
        raise ValueError(f"No data available in previous and next {dt_str} minutes around {time}.")
    idx_closest = np.argmin(np.abs(np.array(list_datetime) - time))
    datetime_closest = list_datetime[idx_closest]
    return datetime_closest


def find_latest_start_time(
    satellite,
    sensor,
    product_level,
    product,
    sector=None,
    connection_type=None,
    base_dir=None,
    protocol="file",
    fs_args={},
    filter_parameters={},
    look_ahead_minutes=30,
):
    """
    Retrieve the latest file start_time available.

    Parameters
    ----------
    look_ahead_minutes: int, optional
        Number of minutes before actual time to search for latest data.
        THe default is 30 minutes.
    base_dir : str, optional
        This argument must be specified only if searching files on the local storage
        when protocol="file".
        It represents the path to the local directory where to search for GOES data.
        If protocol="file" and base_dir is None, base_dir is retrieved from
        the GOES-API config file.
        The default is None.
    protocol : str (optional)
        String specifying the location where to search for the data.
        If protocol="file", it searches on local storage (indicated by base_dir).
        Otherwise, protocol refers to a specific cloud bucket storage.
        Use `goes_api.available_protocols()` to check the available protocols.
        The default is "file".
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `goes_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `goes_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `goes_api.available_products()` for a list of available products.
    sector : str
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    filter_parameters: dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    """
    # Search in the past N hour of data
    start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=look_ahead_minutes)
    end_time = datetime.datetime.utcnow()
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        operational_checks=False,
        connection_type=connection_type,
        verbose=False,
    )
    # Find the latest time available
    if len(fpath_dict) == 0:
        raise ValueError("No data found. Maybe try to increase `look_ahead_minutes`.")
    list_datetime = list(fpath_dict.keys())
    idx_latest = np.argmax(np.array(list_datetime))
    datetime_latest = list_datetime[idx_latest]
    return datetime_latest


def find_closest_files(
    time,
    satellite,
    sensor,
    product_level,
    product,
    sector=None,
    connection_type=None,
    base_dir=None,
    protocol="file",
    fs_args={},
    filter_parameters={},
):
    """
    Retrieve files closest to the specified time.

    If you are querying mesoscale domain data (sector=M), you might be
      interested to specify in the filter_parameters dictionary the
      key `scene_abbr` with values "M1" or "M2".

    Parameters
    ----------
    base_dir : str, optional
        This argument must be specified only if searching files on the local storage
        when protocol="file".
        It represents the path to the local directory where to search for GOES data.
        If protocol="file" and base_dir is None, base_dir is retrieved from
        the GOES-API config file.
        The default is None.
    protocol : str (optional)
        String specifying the location where to search for the data.
        If protocol="file", it searches on local storage (indicated by base_dir).
        Otherwise, protocol refers to a specific cloud bucket storage.
        Use `goes_api.available_protocols()` to check the available protocols.
        The default is "file".
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `goes_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `goes_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `goes_api.available_products()` for a list of available products.
    sector : str
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    time : datetime.datetime
        The time for which you desire to retrieve the files with closest start_time.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `goes_api.available_connection_types` for implemented solutions.
    """
    # Set time precision to minutes
    time = _check_time(time)
    time = time.replace(microsecond=0, second=0)
    # Retrieve timedelta conditioned to sector type
    timedelta = _get_acquisition_max_timedelta(sector)
    # Define start_time and end_time
    start_time = time - timedelta
    end_time = time + timedelta
    # Retrieve files
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        verbose=False,
    )
    # Select start_time closest to time
    list_datetime = sorted(list(fpath_dict.keys()))
    if len(list_datetime) == 0:
        dt_str = int(timedelta.seconds / 60)
        raise ValueError(f"No data available in previous and next {dt_str} minutes around {time}.")
    idx_closest = np.argmin(np.abs(np.array(list_datetime) - time))
    datetime_closest = list_datetime[idx_closest]

    # Retrieve filepaths
    fpaths = fpath_dict[datetime_closest]
    return fpaths


def find_latest_files(
    satellite,
    sensor,
    product_level,
    product,
    sector=None,
    connection_type=None,
    base_dir=None,
    protocol="file",
    fs_args={},
    filter_parameters={},
    N=1,
    operational_checks=True,
    look_ahead_minutes=30,
    return_list=False,
):
    """
    Retrieve latest available files.

    If you are querying mesoscale domain data (sector=M), you might be
      interested to specify in the filter_parameters dictionary the
      key `scene_abbr` with values "M1" or "M2".

    Parameters
    ----------
    look_ahead_minutes: int, optional
        Number of minutes before actual time to search for latest data.
        The default is 30 minutes.
    N : int
        The number of last timesteps for which to download the files.
        The default is 1.
    base_dir : str, optional
        This argument must be specified only if searching files on the local storage
        when protocol="file".
        It represents the path to the local directory where to search for GOES data.
        If protocol="file" and base_dir is None, base_dir is retrieved from
        the GOES-API config file.
        The default is None.
    protocol : str (optional)
        String specifying the location where to search for the data.
        If protocol="file", it searches on local storage (indicated by base_dir).
        Otherwise, protocol refers to a specific cloud bucket storage.
        Use `goes_api.available_protocols()` to check the available protocols.
        The default is "file".
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `goes_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `goes_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `goes_api.available_products()` for a list of available products.
    sector : str
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `goes_api.available_connection_types` for implemented solutions.
    operational_checks: bool, optional
        If True, it checks that:
        1. the file comes from the GOES Operational system Real-time (OR) environment
        2. the scan mode is fixed (for ABI)
        4. the time period between start_time and end_time is fully covered,
            without missing acquisitions.
    """
    # Get closest time
    latest_time = find_latest_start_time(
        look_ahead_minutes=look_ahead_minutes,
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )

    # Find previous files
    # - Dictionary if return_list=True, otherwise a dictionary
    fpaths = find_previous_files(
        N=N,
        start_time=latest_time,
        include_start_time=True,
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
        operational_checks=operational_checks,
        connection_type=connection_type,
        return_list=return_list,
    )
    return fpaths


def find_previous_files(
    start_time,
    N,
    satellite,
    sensor,
    product_level,
    product,
    sector=None,
    filter_parameters={},
    connection_type=None,
    base_dir=None,
    protocol="file",
    fs_args={},
    include_start_time=False,
    operational_checks=True,
    return_list=False,
):
    """
    Find files for N timesteps previous to start_time.

    Parameters
    ----------
    start_time : datetime
        The start_time from which to search for previous files.
    N : int
        The number of previous timesteps for which to retrieve the files.
    include_start_time: bool, optional
        Wheter to include (and count) start_time in the N returned timesteps.
        The default is False.
    base_dir : str, optional
        This argument must be specified only if searching files on the local storage
        when protocol="file".
        It represents the path to the local directory where to search for GOES data.
        If protocol="file" and base_dir is None, base_dir is retrieved from
        the GOES-API config file.
        The default is None.
    protocol : str (optional)
        String specifying the location where to search for the data.
        If protocol="file", it searches on local storage (indicated by base_dir).
        Otherwise, protocol refers to a specific cloud bucket storage.
        Use `goes_api.available_protocols()` to check the available protocols.
        The default is "file".
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `goes_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `goes_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `goes_api.available_products()` for a list of available products.
    sector : str
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `goes_api.available_connection_types` for implemented solutions.
    operational_checks: bool, optional
        If True, it checks that:
        1. the file comes from the GOES Operational system Real-time (OR) environment.
        2. the scan mode is fixed (for ABI).
        3. the start_time is the precise start_time of the file.
        4. there not missing acquisitions between start_time and the previous N.

    Returns
    -------
    fpath_dict : dict
        Dictionary with structure {<datetime>: [fpaths]}

    """
    sensor = _check_sensor(sensor)
    sector = _check_sector(sector, sensor=sensor)
    product_level = _check_product_level(product_level)
    # Set time precision to minutes
    start_time = _check_time(start_time)
    start_time = start_time.replace(microsecond=0, second=0)
    # Get closest time and check is as start_time (otherwise warning)
    closest_time = find_closest_start_time(
        time=start_time,
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )
    # Check start_time is the precise start_time of the file
    if operational_checks and closest_time != start_time:
        raise ValueError(
            f"start_time='{start_time}' is not an actual start_time. " f"The closest start_time is '{closest_time}'",
        )
    # Retrieve timedelta conditioned to sector type
    timedelta = _get_acquisition_max_timedelta(sector)
    # Define start_time and end_time
    start_time = closest_time - timedelta * (N + 1)  # +1 for when include_start_time=False
    end_time = closest_time
    # Retrieve files
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        connection_type=connection_type,
        operational_checks=operational_checks,
        verbose=False,
    )
    # List previous datetime
    list_datetime = sorted(list(fpath_dict.keys()))
    # Remove start_time if include_start_time=False
    if not include_start_time:
        list_datetime.remove(closest_time)
    list_datetime = sorted(list_datetime)
    # Check data availability
    if len(list_datetime) == 0:
        raise ValueError(f"No data available between {start_time} and {end_time}.")
    if len(list_datetime) < N:
        raise ValueError(f"No {N} timesteps available between {start_time} and {end_time}.")
    # Select N most recent start_time
    list_datetime = list_datetime[-N:]
    # Select files for N most recent start_time
    fpath_dict = {tt: fpath_dict[tt] for tt in list_datetime}

    # ----------------------------------------------------------
    # If return_list=True, return list of filepaths (instead of a dictionary)
    if return_list:
        fpaths = list(fpath_dict.values())[0]
        return fpaths
    return fpath_dict


def find_next_files(
    start_time,
    N,
    satellite,
    sensor,
    product_level,
    product,
    sector=None,
    filter_parameters={},
    connection_type=None,
    base_dir=None,
    protocol="file",
    fs_args={},
    include_start_time=False,
    operational_checks=True,
    return_list=False,
):
    """
    Find files for N timesteps after start_time.

    Parameters
    ----------
    start_time : datetime
        The start_time from which search for next files.
    N : int
        The number of next timesteps for which to retrieve the files.
    include_start_time: bool, optional
        Wheter to include (and count) start_time in the N returned timesteps.
        The default is False.
    base_dir : str, optional
        This argument must be specified only if searching files on the local storage
        when protocol="file".
        It represents the path to the local directory where to search for GOES data.
        If protocol="file" and base_dir is None, base_dir is retrieved from
        the GOES-API config file.
        The default is None.
    protocol : str (optional)
        String specifying the location where to search for the data.
        If protocol="file", it searches on local storage (indicated by base_dir).
        Otherwise, protocol refers to a specific cloud bucket storage.
        Use `goes_api.available_protocols()` to check the available protocols.
        The default is "file".
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `goes_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `goes_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `goes_api.available_products()` for a list of available products.
    sector : str
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `goes_api.available_connection_types` for implemented solutions.
    operational_checks: bool, optional
        If True, it checks that:
        1. the file comes from the GOES Operational system Real-time (OR) environment
        2. the scan mode is fixed (for ABI)
        3. the start_time is the precise start_time of the file.
        4. there not missing acquisitions between start_time and the future N.

    Returns
    -------
    fpath_dict : dict
        Dictionary with structure {<datetime>: [fpaths]}

    """
    sensor = _check_sensor(sensor)
    sector = _check_sector(sector, sensor=sensor)
    product_level = _check_product_level(product_level)
    # Set time precision to minutes
    start_time = _check_time(start_time)
    start_time = start_time.replace(microsecond=0, second=0)
    # Get closest time and check is as start_time (otherwise warning)
    closest_time = find_closest_start_time(
        time=start_time,
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )
    # Check start_time is the precise start_time of the file
    if operational_checks and closest_time != start_time:
        raise ValueError(
            f"start_time='{start_time}' is not an actual start_time. " f"The closest start_time is '{closest_time}'",
        )
    # Retrieve timedelta conditioned to sector type
    timedelta = _get_acquisition_max_timedelta(sector)
    # Define start_time and end_time
    start_time = closest_time
    end_time = closest_time + timedelta * (N + 1)  # +1 for when include_start_time=False
    # Retrieve files
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        connection_type=connection_type,
        operational_checks=operational_checks,
        verbose=False,
    )
    # List previous datetime
    list_datetime = sorted(list(fpath_dict.keys()))
    if not include_start_time:
        list_datetime.remove(closest_time)
    list_datetime = sorted(list_datetime)
    # Check data availability
    if len(list_datetime) == 0:
        raise ValueError(f"No data available between {start_time} and {end_time}.")
    if len(list_datetime) < N:
        raise ValueError(f"No {N} timesteps available between {start_time} and {end_time}.")
    # Select N most recent start_time
    list_datetime = list_datetime[0:N]
    # Select files for N most recent start_time
    fpath_dict = {tt: fpath_dict[tt] for tt in list_datetime}

    # ----------------------------------------------------------
    # If return_list=True, return list of filepaths (instead of a dictionary)
    if return_list:
        fpaths = list(fpath_dict.values())[0]
        return fpaths
    return fpath_dict
