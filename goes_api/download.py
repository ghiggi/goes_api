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
"""Define download functions."""

import os
import time
import datetime
import numpy as np
import pandas as pd
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from goes_api.configs import get_goes_base_dir
from goes_api.io import get_filesystem
from goes_api.info import group_files
from goes_api.checks import (
    _check_satellite,
    _check_base_dir, 
    _check_download_protocol,
    _check_year_month,
    _check_time,
    check_date,
)
from goes_api.search import (
    find_files,
    find_closest_start_time,
    find_latest_start_time,
    find_previous_files,
    find_next_files,
)
from goes_api.utils.timing import print_elapsed_time

####--------------------------------------------------------------------------.


def create_local_directories(fpaths, exist_ok=True):
    """Create recursively local directories for the provided filepaths."""
    _ = [os.makedirs(os.path.dirname(fpath), exist_ok=True) for fpath in fpaths]
    return None


def remove_corrupted_files(local_fpaths, bucket_fpaths, fs, return_corrupted_fpaths=True):
    """
    Check and remove files from local disk which are corrupted.

    Corruption is evaluated by comparing the size of data on local storage against
    size of data located in the cloud bucket.

    Parameters
    ----------
    local_fpaths : list
        List of filepaths on local storage.
    bucket_fpaths : list
        List of filepaths on cloud bucket.
    fs : ffspec.FileSystem
        ffspec filesystem instance.
        It must be cohrenet with the cloud bucket address of bucket_fpaths.
    return_corrupted_fpaths : bool, optional
        If True, it returns the list of corrupted files.
        If False, it returns the list of valid files.
        The default is True.

    Returns
    -------
    (list_<valid/corrupted>_local_filepaths, list_<valid/corrupted>_bucket_filepaths)

    """
    l_corrupted_local = []
    l_corrupted_bucket = []
    l_valid_local = []
    l_valid_bucket = []
    for local_fpath, bucket_fpath in zip(local_fpaths, bucket_fpaths):
        local_exists = os.path.isfile(local_fpath)
        if local_exists:
            bucket_size = fs.info(bucket_fpath)["size"]
            local_size = os.path.getsize(local_fpath)
            if bucket_size != local_size:
                os.remove(local_fpath)
                l_corrupted_local.append(local_fpath)
                l_corrupted_bucket.append(bucket_fpath)
            else:
                l_valid_local.append(local_fpath)
                l_valid_bucket.append(bucket_fpath)
    if return_corrupted_fpaths:
        return l_corrupted_local, l_corrupted_bucket
    else:
        return l_valid_local, l_valid_bucket





def _select_missing_fpaths(local_fpaths, bucket_fpaths):
    """Return local and bucket filepaths of files not present on the local storage."""
    # Keep only non-existing local files
    idx_not_exist = [not os.path.exists(filepath) for filepath in local_fpaths]
    local_fpaths = list(np.array(local_fpaths)[idx_not_exist])
    bucket_fpaths = list(np.array(bucket_fpaths)[idx_not_exist])
    return local_fpaths, bucket_fpaths


def _remove_bucket_address(fpath):
    """Remove the bucket acronym (i.e. s3://, gcs://) from the file path."""
    fel = fpath.split("/")[3:]
    fpath = os.path.join(*fel)
    return fpath


def _get_local_from_bucket_fpaths(base_dir, satellite, bucket_fpaths):
    """Convert cloud bucket filepaths to local storage filepaths."""
    satellite = satellite.upper()
    fpaths = [
        os.path.join(base_dir, satellite, _remove_bucket_address(fpath))
        for fpath in bucket_fpaths
    ]
    return fpaths


def _fs_get_parallel(bucket_fpaths, local_fpaths, fs, n_threads=10, progress_bar=True):
    """
    Run fs.get() asynchronously in parallel using multithreading.

    Parameters
    ----------
    bucket_fpaths : list
        List of bucket filepaths to download.
    local_fpath : list
        List of filepaths where to save data on local storage.
    n_threads : int, optional
        Number of files to be downloaded concurrently.
        The default is 10. The max value is set automatically to 50.

    Returns
    -------
    List of cloud bucket filepaths which were not downloaded.
    """
    # Check n_threads
    if n_threads < 1:
        n_threads = 1
    n_threads = min(n_threads, 50)

    ##------------------------------------------------------------------------.
    # Initialize progress bar
    if progress_bar:
        n_files = len(local_fpaths)
        pbar = tqdm(total=n_files)
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        dict_futures = {
            executor.submit(fs.get, bucket_path, local_fpath): bucket_path
            for bucket_path, local_fpath in zip(bucket_fpaths, local_fpaths)
        }
        # List files that didn't work
        l_file_error = []
        for future in concurrent.futures.as_completed(dict_futures.keys()):
            # Update the progress bar
            if progress_bar:
                pbar.update(1)
            # Collect all commands that caused problems
            if future.exception() is not None:
                l_file_error.append(dict_futures[future])
    if progress_bar:
        pbar.close()
    ##------------------------------------------------------------------------.
    # Return list of bucket fpaths raising errors
    return l_file_error


def _get_end_of_day(time):
    """Get datetime end of the day."""
    time_end_of_day = time + datetime.timedelta(days=1)
    time_end_of_day = time_end_of_day.replace(hour=0, minute=0, second=0)
    return time_end_of_day


def _get_start_of_day(time):
    """Get datetime start of the day."""
    time_start_of_day = time
    time_start_of_day = time_start_of_day.replace(hour=0, minute=0, second=0)
    return time_start_of_day


def _get_list_daily_time_blocks(start_time, end_time):
    """Return a list of (start_time, end_time) tuple of daily length."""
    # Retrieve timedelta between start_time and end_time
    dt = end_time - start_time
    # If less than a day
    if dt.days == 0:
        return [(start_time, end_time)]
    # Otherwise split into daily blocks (first and last can be shorter)
    end_of_start_time = _get_end_of_day(start_time)
    start_of_end_time = _get_start_of_day(end_time)
    # Define list of daily blocks
    l_steps = pd.date_range(end_of_start_time, start_of_end_time, freq="1D")
    l_steps = l_steps.to_pydatetime().tolist()
    l_steps.insert(0, start_time)
    l_steps.append(end_time)
    l_daily_blocks = [(l_steps[i], l_steps[i + 1]) for i in range(0, len(l_steps) - 1)]
    return l_daily_blocks


def _enable_multiple_products(func): 
    """Decorator to retrieve filepaths for multiple products."""
    def decorator(*args, **kwargs):
        # Single product case
        if isinstance(kwargs['product'], str):
            fpaths = func(*args, **kwargs)
            return fpaths

        # Multiproduct case
        elif isinstance(kwargs['product'], list): 
            products = kwargs['product']
            list_fpaths = []
            for product in products:
                new_kwargs = kwargs.copy()
                new_kwargs['product'] = product
                fpaths = func(*args, **new_kwargs)
                list_fpaths.append(fpaths)
            # Flat the list 
            fpaths = [item for sublist in list_fpaths for item in sublist]   
            return fpaths
        else: 
            raise ValueError("Expecting 'product' to be a string or a list.")
    return decorator


####---------------------------------------------------------------------------.
@print_elapsed_time
@_enable_multiple_products
def download_files(
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    start_time,
    end_time,
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
    filter_parameters={},
    base_dir = None, 
    fs_args={},
):
    """
    Download files from a cloud bucket storage.

    Parameters
    ----------
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data.
        Use `goes_api.available_protocols()` to retrieve available protocols.
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
    base_dir : str, optional
        The path to the directory where to store GOES data. 
        If None, it use the one specified  in the GOES-API config file.
        The default is None.
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    n_threads: int
        Number of files to be downloaded concurrently.
        The default is 20. The max value is set automatically to 50.
    force_download: bool
        If True, it downloads and overwrites the files already existing on local storage.
        If False, it does not downloads files already existing on local storage.
        The default is False.
    check_data_integrity: bool
        If True, it checks that the downloaded files are not corrupted.
        Corruption is assessed by comparing file size between local and cloud bucket storage.
        The default is True.
    progress_bar: bool
        If True, it displays a progress bar showing the download status.
        The default is True.
    verbose : bool, optional
        If True, it print some information concerning the download process.
        The default is False.

    """
    # -------------------------------------------------------------------------.
    # Get default directory 
    base_dir = get_goes_base_dir(base_dir)
    # Checks
    _check_download_protocol(protocol)
    base_dir = _check_base_dir(base_dir)
    satellite = _check_satellite(satellite)
    start_time = _check_time(start_time)
    end_time = _check_time(end_time)
    
    # Initialize timing
    t_i = time.time()
    
    # -------------------------------------------------------------------------.
    # Get filesystem
    fs = get_filesystem(protocol=protocol, fs_args=fs_args)

    # Define list of daily time blocks (start_time, end_time)
    time_blocks = _get_list_daily_time_blocks(start_time, end_time)

    if verbose:
        print("-------------------------------------------------------------------- ")
        print(f"Starting downloading {product} data between {start_time} and {end_time}.")

    # Loop over daily time blocks (to search for data)
    list_all_local_fpaths = []
    list_all_bucket_fpaths = []
    n_downloaded_files = 0
    for start_time, end_time in time_blocks:
        # Retrieve bucket fpaths
        bucket_fpaths = find_files(
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
            connection_type="bucket",
            operational_checks=False, 
            base_dir=None,
            group_by_key=None,
            verbose=False,
        )
        # Check there are files to retrieve
        n_files = len(bucket_fpaths)
        if n_files == 0:
            continue

        # Define local destination fpaths
        local_fpaths = _get_local_from_bucket_fpaths(
            base_dir=base_dir, satellite=satellite, bucket_fpaths=bucket_fpaths
        )

        # Record the local and bucket fpath queried
        list_all_local_fpaths = list_all_local_fpaths + local_fpaths
        list_all_bucket_fpaths = list_all_bucket_fpaths + bucket_fpaths

        # Remove corrupted data
        _ = remove_corrupted_files(
            local_fpaths=local_fpaths, bucket_fpaths=bucket_fpaths, fs=fs
        )

        # Optionally exclude files that already exist on disk
        if not force_download:
            local_fpaths, bucket_fpaths = _select_missing_fpaths(
                local_fpaths=local_fpaths, bucket_fpaths=bucket_fpaths
            )

        # Check there are still files to retrieve
        n_files = len(local_fpaths)
        n_downloaded_files += n_files
        if n_files == 0:
            continue

        # Create local directories
        create_local_directories(local_fpaths)

        # Print # files to download
        if verbose:
            print(f" - Downloading {n_files} files from {start_time} to {end_time}")

        # Download data asynchronously with multithreading
        l_bucket_errors = _fs_get_parallel(
            bucket_fpaths=bucket_fpaths,
            local_fpaths=local_fpaths,
            fs=fs,
            n_threads=n_threads,
            progress_bar=progress_bar,
        )
        # Report errors if occured
        if verbose:
            n_errors = len(l_bucket_errors)
            if n_errors > 0:
                print(f" - Unable to download the following files: {l_bucket_errors}")

    # Report the total number of file downloaded
    if verbose:
        t_f = time.time()
        t_elapsed = round(t_f - t_i)
        print(
            f"--> {n_downloaded_files} files have been downloaded in {t_elapsed} seconds !"
        )
        print("-------------------------------------------------------------------- ")

    # Check for data corruption
    if check_data_integrity:
        if verbose:
            print("Checking data integrity:")
        list_all_local_fpaths, _ = remove_corrupted_files(
            list_all_local_fpaths,
            list_all_bucket_fpaths,
            fs=fs,
            return_corrupted_fpaths=False,
        )
        if verbose:
            n_corrupted = len(list_all_bucket_fpaths) - len(list_all_local_fpaths)
            print(f" - {n_corrupted} corrupted files were identified and removed.")
            print(
                "--------------------------------------------------------------------"
            )

    # Return list of local fpaths
    return list_all_local_fpaths


####---------------------------------------------------------------------------.


def download_closest_files(
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    time,
    sector=None,
    filter_parameters={},
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
    fs_args={},
    base_dir=None,
):
    """
    Download files from a cloud bucket storage closest to the specified time.

    Parameters
    ----------
    time : datetime.datetime
        The time for which you desire to retrieve the files with closest start_time.
    base_dir : str
        Base directory path where the <GOES-**>/<product>/... directory structure
        should be created.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data.
        Use `goes_api.available_protocols()` to retrieve available protocols.
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
    n_threads: int
        Number of files to be downloaded concurrently.
        The default is 20. The max value is set automatically to 50.
    force_download: bool
        If True, it downloads and overwrites the files already existing on local storage.
        If False, it does not downloads files already existing on local storage.
        The default is False.
    check_data_integrity: bool
        If True, it checks that the downloaded files are not corrupted.
        Corruption is assessed by comparing file size between local and cloud bucket storage.
        The default is True.
    progress_bar: bool
        If True, it displays a progress bar showing the download status.
        The default is True.
    verbose : bool, optional
        If True, it print some information concerning the download process.
        The default is False.
    base_dir : str (optional)
        The path to the directory where to store GOES data. 
        If None, it use the one specified  in the GOES-API config file.
        The default is None.
    """
    # Checks
    _check_download_protocol(protocol)
    # Get closest time
    closest_time = find_closest_start_time(
        time=time,
        base_dir=None,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )
    # Download files
    fpaths = download_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
        start_time=closest_time,
        end_time=closest_time,
        n_threads=n_threads,
        force_download=force_download,
        progress_bar=progress_bar,
        check_data_integrity=check_data_integrity,
        verbose=verbose,
    )
    return fpaths


def download_latest_files(
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    sector=None,
    filter_parameters={},
    N = 1, 
    check_consistency=True,
    look_ahead_minutes=30, 
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
    fs_args={},
    base_dir=None,
    return_list=False,
):
    """
    Download latest available files from a cloud bucket storage.

    Parameters
    ----------
    look_ahead_minutes: int, optional
        Number of minutes before actual time to search for latest data.
        The default is 30 minutes
    N : int
        The number of last timesteps for which to download the files.
        The default is 1.
    check_consistency : bool, optional
        Check for consistency of the returned files. The default is True.
        It check that:
         - the regularity of the previous timesteps, with no missing timesteps;
         - the regularity of the scan mode, i.e. not switching from M3 to M6,
         - if sector == M, the mesoscale domains are not changing within the considered period.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data.
        Use `goes_api.available_protocols()` to retrieve available protocols.
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
    n_threads: int
        Number of files to be downloaded concurrently.
        The default is 20. The max value is set automatically to 50.
    force_download: bool
        If True, it downloads and overwrites the files already existing on local storage.
        If False, it does not downloads files already existing on local storage.
        The default is False.
    check_data_integrity: bool
        If True, it checks that the downloaded files are not corrupted.
        Corruption is assessed by comparing file size between local and cloud bucket storage.
        The default is True.
    progress_bar: bool
        If True, it displays a progress bar showing the download status.
        The default is True.
    verbose : bool, optional
        If True, it print some information concerning the download process.
        The default is False.
    base_dir : str (optional)
        The path to the directory where to store GOES data. 
        If None, it use the one specified  in the GOES-API config file.
        The default is None.
    """
    # Checks
    _check_download_protocol(protocol)
    # Get closest time
    latest_start_time = find_latest_start_time(
        look_ahead_minutes=look_ahead_minutes,
        base_dir=None,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )
    # Download files
    fpaths = download_previous_files(
        N = N, 
        check_consistency=check_consistency,
        start_time=latest_start_time,
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
        n_threads=n_threads,
        force_download=force_download,
        check_data_integrity=check_data_integrity,
        verbose=verbose,
        return_list=return_list,
    )
    return fpaths


def download_previous_files(
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    start_time,
    N,
    sector=None,
    filter_parameters={},
    include_start_time=False,
    check_consistency=True,
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
    fs_args={},
    base_dir=None,
    return_list=False,
):
    """
    Download files for N timesteps previous to start_time.

    Parameters
    ----------
    start_time : datetime
        The start_time from which to search for previous files.
        The start_time should correspond exactly to file start_time if check_consistency=True
    N : int
        The number of previous timesteps for which to download the files.
    include_start_time: bool, optional
        Wheter to include (and count) start_time in the N timesteps for which
        file are downloaded.
        The default is False.
    check_consistency : bool, optional
        Check for consistency of the returned files. The default is True.
        It check that:
         - start_time correspond exactly to the start_time of the files;
         - the regularity of the previous timesteps, with no missing timesteps;
         - the regularity of the scan mode, i.e. not switching from M3 to M6,
         - if sector == M, the mesoscale domains are not changing within the considered period.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data.
        Use `goes_api.available_protocols()` to retrieve available protocols.
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
    n_threads: int
        Number of files to be downloaded concurrently.
        The default is 20. The max value is set automatically to 50.
    force_download: bool
        If True, it downloads and overwrites the files already existing on local storage.
        If False, it does not downloads files already existing on local storage.
        The default is False.
    check_data_integrity: bool
        If True, it checks that the downloaded files are not corrupted.
        Corruption is assessed by comparing file size between local and cloud bucket storage.
        The default is True.
    progress_bar: bool
        If True, it displays a progress bar showing the download status.
        The default is True.
    verbose : bool, optional
        If True, it print some information concerning the download process.
        The default is False.
    base_dir : str (optional)
        The path to the directory where to store GOES data. 
        If None, it use the one specified  in the GOES-API config file.
        The default is None.
    """
    # Checks
    _check_download_protocol(protocol)
    # Get previous files dictionary
    fpath_dict = find_previous_files(
        base_dir=None,
        start_time=start_time,
        N=N, 
        include_start_time=include_start_time,
        check_consistency=check_consistency,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )
    # Get list datetime
    list_datetime = list(fpath_dict.keys())
    start_time = list_datetime[0]
    end_time = list_datetime[-1]   
    
    # Download files
    fpaths = download_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
        start_time=start_time,
        end_time=end_time,
        n_threads=n_threads,
        force_download=force_download,
        check_data_integrity=check_data_integrity,
        verbose=verbose,
    )
    # If return_list=False, group files by start_time
    if not return_list:
        fpaths = group_files(fpaths, key="start_time")
    return fpaths


def download_next_files(
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    start_time,
    N,
    sector=None,
    filter_parameters={},
    include_start_time=False,
    check_consistency=True,
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
    fs_args={},
    base_dir=None,
    return_list=False,
):
    """
    Download files for N timesteps after start_time.

    Parameters
    ----------
    start_time : datetime
        The start_time from which search for next files.
        The start_time should correspond exactly to file start_time if check_consistency=True
    N : int
        The number of next timesteps for which to retrieve the files.
    include_start_time: bool, optional
        Wheter to include (and count) start_time in the N returned timesteps.
        The default is False.
    check_consistency : bool, optional
        Check for consistency of the returned files. The default is True.
        It check that:
         - start_time correspond exactly to the start_time of the files;
         - the regularity of the previous timesteps, with no missing timesteps;
         - the regularity of the scan mode, i.e. not switching from M3 to M6,
         - if sector == M, the mesoscale domains are not changing within the considered period.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data.
        Use `goes_api.available_protocols()` to retrieve available protocols.
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
    n_threads: int
        Number of files to be downloaded concurrently.
        The default is 20. The max value is set automatically to 50.
    force_download: bool
        If True, it downloads and overwrites the files already existing on local storage.
        If False, it does not downloads files already existing on local storage.
        The default is False.
    check_data_integrity: bool
        If True, it checks that the downloaded files are not corrupted.
        Corruption is assessed by comparing file size between local and cloud bucket storage.
        The default is True.
    progress_bar: bool
        If True, it displays a progress bar showing the download status.
        The default is True.
    verbose : bool, optional
        If True, it print some information concerning the download process.
        The default is False.
    base_dir : str (optional)
        The path to the directory where to store GOES data. 
        If None, it use the one specified  in the GOES-API config file.
        The default is None.
    """
    # Checks
    _check_download_protocol(protocol)
    # Get previous files dictionary
    fpath_dict = find_next_files(
        base_dir=None,
        start_time=start_time,
        N=N,
        include_start_time=include_start_time,
        check_consistency=check_consistency,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )
    # Get list datetime
    list_datetime = list(fpath_dict.keys())
    start_time = list_datetime[0]
    end_time = list_datetime[-1]   
    # Download files
    fpaths = download_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
        start_time=start_time,
        end_time=end_time,
        n_threads=n_threads,
        force_download=force_download,
        check_data_integrity=check_data_integrity,
        verbose=verbose,
    )
    # If return_list=False, group files by start_time
    if not return_list:
        fpaths = group_files(fpaths, key="start_time")
    return fpaths


####---------------------------------------------------------------------------.
def download_monthly_files(
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    year,
    month,
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
    filter_parameters={},
    fs_args={},
    base_dir=None,
): 
    """
    Download monthly files from a cloud bucket storage.

    Parameters
    ----------
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data.
        Use `goes_api.available_protocols()` to retrieve available protocols.
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
    sector:
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    year : int
        Year of the month for which to download the data.
    month : int
        Month of the year for which to download the data.

    n_threads: int
        Number of files to be downloaded concurrently.
        The default is 20. The max value is set automatically to 50.    
    force_download: bool
        If True, it downloads and overwrites the files already existing on local storage.
        If False, it does not downloads files already existing on local storage.
        The default is False.
    check_data_integrity: bool
        If True, it checks that the downloaded files are not corrupted.
        Corruption is assessed by comparing file size between local and cloud bucket storage.
        The default is True.
    progress_bar: bool
        If True, it displays a progress bar showing the download status.
        The default is True.
    verbose : bool, optional
        If True, it print some information concerning the download process.
        The default is False.
    base_dir : str (optional)
        The path to the directory where to store GOES data. 
        If None, it use the one specified  in the GOES-API config file.
        The default is None.

    """
    # -------------------------------------------------------------------------.
    # Checks
    from dateutil.relativedelta import relativedelta
    year, month = _check_year_month(year, month)
    start_time = datetime.datetime(year, month, 1, 0, 0, 0)
    end_time = start_time + relativedelta(months=1)
    list_all_local_fpaths = download_files(
        base_dir=base_dir,
        protocol=protocol, 
        satellite=satellite, 
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector, 
        start_time=start_time,
        end_time=end_time,
        n_threads=n_threads,
        force_download=force_download,
        check_data_integrity=check_data_integrity,
        progress_bar=progress_bar,
        verbose=verbose,
        filter_parameters=filter_parameters,
        fs_args=fs_args,
    )
    return list_all_local_fpaths


def download_daily_files(
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    date, 
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
    filter_parameters={},
    fs_args={},
    base_dir=None,
    ): 
    """
    Download daily files from a cloud bucket storage.

    Parameters
    ----------
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data.
        Use `goes_api.available_protocols()` to retrieve available protocols.
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
    sector:
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    date : datetime.date
        Date for which to download the data 
    n_threads: int
        Number of files to be downloaded concurrently.
        The default is 20. The max value is set automatically to 50.    
    force_download: bool
        If True, it downloads and overwrites the files already existing on local storage.
        If False, it does not downloads files already existing on local storage.
        The default is False.
    check_data_integrity: bool
        If True, it checks that the downloaded files are not corrupted.
        Corruption is assessed by comparing file size between local and cloud bucket storage.
        The default is True.
    progress_bar: bool
        If True, it displays a progress bar showing the download status.
        The default is True.
    verbose : bool, optional
        If True, it print some information concerning the download process.
        The default is False.
    base_dir : str (optional)
        The path to the directory where to store GOES data. 
        If None, it use the one specified  in the GOES-API config file.
        The default is None.
    """
    # -------------------------------------------------------------------------.
    # Checks
    from dateutil.relativedelta import relativedelta
    
    start_time = check_date(date) 
    end_time = start_time + relativedelta(days=1)
    list_all_local_fpaths = download_files(
        base_dir=base_dir,
        protocol=protocol, 
        satellite=satellite, 
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector, 
        start_time=start_time,
        end_time=end_time,
        n_threads=n_threads,
        force_download=force_download,
        check_data_integrity=check_data_integrity,
        progress_bar=progress_bar,
        verbose=verbose,
        filter_parameters=filter_parameters,
        fs_args=fs_args,
    )
    return list_all_local_fpaths