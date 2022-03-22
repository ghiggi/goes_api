#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 16:31:24 2022

@author: ghiggi
"""
import os
import time
import dask
import ujson
import fsspec
import concurrent.futures
from tqdm import tqdm
from kerchunk.hdf import SingleHdf5ToZarr 
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from .utils.time import get_list_daily_time_blocks
from .io import (
    infer_satellite_from_path,
    remove_bucket_address,
    find_files
)


def _generate_reference_json(url, reference_dir, fs_args={}):
    """Derive the kerchunk reference JSON file. 
    
    The file is saved at <reference_dir>/<satellite>/.../*.nc.json
    """
    # Retrieve satellite
    satellite = infer_satellite_from_path(url)
    satellite = satellite.upper() # GOES-16/GOES-17
    
    # Define output json fpath 
    standard_path = remove_bucket_address(url)   
    reference_fpath = os.path.join(reference_dir, satellite, standard_path + ".json")
    
    # Create directory
    os.makedirs(os.path.dirname(reference_fpath), exist_ok=True)
    
    # Read remote file and retrieve kerchunk reference dictionary 
    with fsspec.open(url, **fs_args) as input_f:
        h5chunks = SingleHdf5ToZarr(input_f, url, inline_threshold=200)
        file_metadata = h5chunks.translate()
        # Write kerchunk reference dictionary to JSON file 
        with open(reference_fpath, 'wb') as output_f:
            output_f.write(ujson.dumps(file_metadata).encode())

    return None

def _get_parallel_ref(bucket_fpaths, fs_args, reference_dir, 
                      n_processes=20, progress_bar=True):
    """
    Run _generate_reference_json asynchronously in parallel using multiprocessing.

    Parameters
    ----------
    bucket_fpaths : list
        List of bucket filepaths to derive kerchunk reference JSON dictionary.
    n_processes : int, optional
        Number of files to be analyzed concurrently.
        The default is 20. The max value is set automatically to 50.

    Returns
    -------
    List of cloud bucket filepaths which were not analyzed.
   
    """
    # Check n_threads
    if n_processes < 1:
        n_processes = 1
    n_processes = min(n_processes, 50)

    ##------------------------------------------------------------------------.
    # Initialize progress bar
    if progress_bar:
        n_files = len(bucket_fpaths)
        pbar = tqdm(total=n_files)
    with ThreadPoolExecutor(max_workers=n_processes) as executor:
    # with ProcessPoolExecutor(max_workers=n_processes) as executor:
        dict_futures = {
            executor.submit(_generate_reference_json, bucket_path, reference_dir, fs_args): bucket_path
            for bucket_path in bucket_fpaths
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

def generate_kerchunk_files(
        satellite,
        sensor,
        product_level,
        product,
        sector,
        start_time,
        end_time,
        filter_parameters={},
        reference_dir=None,
        n_processes=20, 
        protocol=None,
        fs_args={},
        verbose=False,
        progress_bar=True, 
        ):
    
    # Define fs_args for kerchunking 
    kerchunk_fs_arg = fs_args.copy()
    kerchunk_fs_arg['mode'] = "rb"
    kerchunk_fs_arg['anon'] = "True"
    kerchunk_fs_arg['default_fill_cache'] = "False"
    kerchunk_fs_arg['default_cache_type'] = "none"
    
    # Define list of daily time blocks (start_time, end_time)
    time_blocks = get_list_daily_time_blocks(start_time, end_time)

    if verbose:
        # Initialize timing
        t_i = time.time()
        print("-------------------------------------------------------------------- ")
        print(f"Starting kerchunking data between {start_time} and {end_time}.")

    # Loop over daily time blocks (to search for data)
    n_kerchunked_files = 0
    for start_time, end_time in time_blocks:
        
        # Retrieve filepaths to derive kerchunk reference JSON file  
        fpaths = find_files(
            base_dir=None,
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
            connection_type=None,
            group_by_key=None,
            verbose=verbose,
        )
    
        # Check there are files to process
        n_files = len(fpaths)
        n_kerchunked_files += n_files
        if n_files == 0:
            continue
        
        if verbose:
            print(f" - Kerchunking {n_files} files from {start_time} to {end_time}")
        
        # Compute and write JSON files with dask  [OPTION 1]
        delayed_gen_fun = dask.delayed(_generate_reference_json)
        out = [delayed_gen_fun(fpath,
                                reference_dir=reference_dir, 
                                fs_args=fs_args) for fpath in fpaths]
        dask.compute(out)
        
        # Compute and write JSON files concurrently 
        # l_file_error = _get_parallel_ref(bucket_fpaths=fpaths,
        #                                  fs_args=kerchunk_fs_arg,
        #                                  reference_dir=reference_dir, 
        #                                  n_processes=n_processes, 
        #                                  progress_bar=progress_bar)
        
        # Report errors if occured
        # if verbose:
        #     n_errors = len(l_file_error)
        #     if n_errors > 0:
        #         print(f" - Unable to kerchunk the following files: {l_file_error}")
        
    # Report the total number of file kerchunked
    if verbose:
        t_f = time.time()
        t_elapsed = round(t_f - t_i)
        print(
            f"--> {n_kerchunked_files} files have been kerchunked in {t_elapsed} seconds !"
        )
        print("-------------------------------------------------------------------- ")         
                 
    return None 


def get_reference_mappers(fpaths, protocol="s3"):
    """Return list of reference mappers objects."""
    m_list = []
    for fpath in tqdm(fpaths):
        # Open reference dict
        with open(fpath) as f:
        
            reference_dict = ujson.load(f)
        # TODO: here possibly change bucket url 
        reference_dict = reference_dict.copy()
        # Create FSMap 
        m_list.append(fsspec.get_mapper("reference://", 
                        fo=reference_dict,
                        remote_protocol=protocol,
                        remote_options={'anon':True}))
    return m_list