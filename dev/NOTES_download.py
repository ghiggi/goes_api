#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 11:04:16 2022

@author: ghiggi
"""
#-----------------------------------------------------------------------------.
# Raw data storage requirements for 1 satellite:
# 4 GB per hour
# 100-120 GB per day 
# 3 TB per month

#-----------------------------------------------------------------------------.
# https://github.com/blaylockbk/pyBKB_v3/blob/master/rclone_howto.md 
# curl https://rclone.org/install.sh | sudo bash
# sudo apt update; sudo apt install -y libgnutls30

#-----------------------------------------------------------------------------.
# DOY 
# first july: 182 
# 15 august: 227

import time
import datetime 
import subprocess
from goes_api.io import (
    _get_product_dir,
    _get_bucket_prefix,
    _get_time_dir_tree,
)
from goes_api.download import _get_local_from_bucket_fpaths
from goes_api.checks import (
     _check_download_protocol,
     _check_protocol,
     _check_base_dir,
     _check_satellite,
     _check_sensor,
     _check_product_level,
     _check_year_month,
     _check_product,
     _check_sector,
)

base_dir = "/ltenas8/data/GEO"
protocol = "gcs"
protocol = "s3"

###---------------------------------------------------------------------------.
#### Define satellite, sensor, product_level and product
satellite = "GOES-16"
sensor = "ABI"
sector = "F"
product_level = "L2"
product = "RRQPE"
year = 2022
month = 7
 
 



#### RCLONE 
def download_daily_directory(
def download_monthly_directory(
    base_dir,
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
):
    """
    Donwload files from a cloud bucket storage directory using rclone.

    Parameters
    ----------
    base_dir : str
        Base directory path where the <GOES-**>/<product>/... directory structure
        should be created.
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

    """
    # -------------------------------------------------------------------------.
    # Checks
    _check_download_protocol(protocol)
    base_dir = _check_base_dir(base_dir)
    satellite = _check_satellite(satellite)
    sensor = _check_sensor(sensor)
    product_level = _check_product_level(product_level, product=None)
    product = _check_product(product, sensor=sensor, product_level=product_level)
    sector = _check_sector(sector, product=product, sensor=sensor)
    year, month = _check_year_month(year, month)
       
    # Initialize timing
    t_i = time.time()

    # -------------------------------------------------------------------------.
    # Get bucket address 
    bucket_prefix = _get_bucket_prefix(protocol)

    # Get product dir
    product_dir = _get_product_dir(
        protocol=protocol,
        base_dir=None, 
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
    )
    # s3:// publicAWS:
        
    # Define bucket directory 
    # doy - folders 
    month_str = str(month).rjust(2,"0")
    bucket_dir = os.path.join(product_dir, str(year), month_str)
    
    
    local_dir = _get_local_from_bucket_fpaths(
         base_dir=base_dir, satellite=satellite, bucket_fpaths=[bucket_dir]
     )[0]
    local_dir
    bucket_dir
 
# --transfers=N  # number of files to run in parallel (default 4)
# --size-only    # compare file size 
# --checksum     # (slower than --size-only)

# https://rclone.org/s3/
# https://rclone.org/googlecloudstorage/

# https://github.com/blaylockbk/pyBKB_v3/blob/master/rclone_howto.md 

import os 
for doy in range(187, 227):
    src_fpath = "publicAWS:noaa-goes16/ABI-L1b-RadF/2020/" + str(doy)
    dst_fpath = "/ltenas3/0_Data/GEO/GOES16/ABI-L1b-RadF/2020/" + str(doy)
    cmd_fpath = "rclone copy " + src_fpath + " " + dst_fpath
    print(doy)
    os.system(cmd_fpath)


download_rclone_daily
download_rclone_year

# rclone always works as if you had written a trailing / 
# --> meaning "copy the contents of this directory"






## Download GOES data 
# for doy in {001...365} do aws s3 sync s3://noaa-goes16/blahblah ./ & done 

#-----------------------------------------------------------------------------.
#### Scripted/bulk downloading
### TO download
# rclone, s3fs
# GOES-2-Go: https://github.com/blaylockbk/goes2go
# goespy: https://github.com/spestana/goes-py
# goes-mirror: https://github.com/meteoswiss-mdr/goesmirror
# GOES: https://github.com/joaohenry23/GOES/tree/master/GOES
# https://github.com/blaylockbk/pyBKB_v3/blob/master/rclone_howto.md
# Satpy and GOES-2-Go packages automate the generation of common GOES-R composites

## Interactive download 
# https://home.chpc.utah.edu/~u0553130/Brian_Blaylock/cgi-bin/goes16_download.cgi
# https://home.chpc.utah.edu/~u0553130/Brian_Blaylock/cgi-bin/generic_AWS_download.cgi?DATASET=noaa-goes16