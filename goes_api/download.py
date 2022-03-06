#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 13:21:15 2022

@author: ghiggi
"""
import os
import time
import dask
import numpy as np
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from .utils.time import get_list_daily_time_blocks
from .io import (
    get_filesystem,
    find_files,
    find_closest_start_time,
    find_latest_start_time,
    group_files,
    find_previous_files,
    find_next_files,
    _check_satellite,
    _check_base_dir,
)

####--------------------------------------------------------------------------.


def create_local_directories(fpaths, exist_ok=True):
    _ = [os.makedirs(os.path.dirname(fpath), exist_ok=True) for fpath in fpaths]
    return None


def rm_corrupted_files(local_fpaths, bucket_fpaths, fs, return_corrupted_fpaths=True):
    l_corrupted_local = []
    l_corrupted_bucket = []
    l_valid_local = []
    l_valid_bucket = []
    for local_fpath, bucket_fpath in zip(local_fpaths, bucket_fpaths):
        local_exists = os.path.isfile(local_fpath)
        if local_exists:
            bucket_size = fs.info(bucket_fpath)["Size"]
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


def _check_download_protocol(protocol):
    if protocol not in ["gcs", "s3"]:
        raise ValueError("Please specify either 'gcs' or 's3' protocol for download.")


def _select_missing_fpaths(local_fpaths, bucket_fpaths):
    # Keep only non-existing local files
    idx_not_exist = [not os.path.exists(filepath) for filepath in local_fpaths]
    local_fpaths = list(np.array(local_fpaths)[idx_not_exist])
    bucket_fpaths = list(np.array(bucket_fpaths)[idx_not_exist])
    return local_fpaths, bucket_fpaths


def _rm_bucket_address(fpath):
    fel = fpath.split("/")[3:]
    fpath = os.path.join(*fel)
    return fpath


def _get_local_from_bucket_fpaths(base_dir, satellite, bucket_fpaths):
    satellite = satellite.upper()
    fpaths = [
        os.path.join(base_dir, satellite, _rm_bucket_address(fpath))
        for fpath in bucket_fpaths
    ]
    return fpaths


def _fs_get_parallel(bucket_fpaths, local_fpaths, fs, n_threads=10, progress_bar=True):
    """
    Run fs.get download asynchronously in parallel using multithreading.

    Parameters
    ----------
    bucket_fpaths : list
        List of bucket fpaths to download.
    local_fpath : list
        List of fpaths where to save locally the data.
    n_threads : int, optional
        Number of parallel download. The default is 10.

    Returns
    -------
    List of commands which didn't complete.
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


####---------------------------------------------------------------------------.
def download_files(
    base_dir,
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
    fs_args={},
):
    # -------------------------------------------------------------------------.
    # Checks
    _check_download_protocol(protocol)
    base_dir = _check_base_dir(base_dir)
    satellite = _check_satellite(satellite)

    # Initialize timing
    t_i = time.time()

    # -------------------------------------------------------------------------.
    # Get filesystem
    fs = get_filesystem(protocol=protocol, fs_args=fs_args)

    # Define list of daily time blocks (start_time, end_time)
    time_blocks = get_list_daily_time_blocks(start_time, end_time)

    if verbose:
        print("-------------------------------------------------------------------- ")
        print(f"Starting downloading data between {start_time} and {end_time}.")

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
        _ = rm_corrupted_files(
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
        list_all_local_fpaths, _ = rm_corrupted_files(
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


def download_closest_files(
    base_dir,
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    time,
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    verbose=True,
    filter_parameters={},
    fs_args={},
):
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
        check_data_integrity=check_data_integrity,
        verbose=verbose,
    )
    return fpaths


def download_latest_files(
    base_dir,
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    verbose=True,
    filter_parameters={},
    fs_args={},
):
    # Checks
    _check_download_protocol(protocol)
    # Get closest time
    latest_time = find_latest_start_time(
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
        start_time=latest_time,
        end_time=latest_time,
        n_threads=n_threads,
        force_download=force_download,
        check_data_integrity=check_data_integrity,
        verbose=verbose,
    )
    return fpaths


def download_previous_files(
    base_dir,
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    start_time,
    N,
    include_start_time=False,
    check_consistency=True,
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    verbose=True,
    filter_parameters={},
    fs_args={},
):
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
    end_time = list_datetime[-1]  # TODO: select end_time maybe
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
    # Group files by start_time
    fpaths_dict = group_files(fpaths, sensor, product_level, key="start_time")
    return fpaths_dict


def download_next_files(
    base_dir,
    protocol,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    start_time,
    N,
    include_start_time=False,
    check_consistency=True,
    n_threads=20,
    force_download=False,
    check_data_integrity=True,
    verbose=True,
    filter_parameters={},
    fs_args={},
):
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
    end_time = list_datetime[-1]  # TODO: select end_time maybe
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
    # Group files by start_time
    fpaths_dict = group_files(fpaths, sensor, product_level, key="start_time")
    return fpaths_dict
