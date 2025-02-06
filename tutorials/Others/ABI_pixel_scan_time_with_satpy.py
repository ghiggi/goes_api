#!/usr/bin/env python3
"""
Created on Tue Apr 26 12:13:15 2022

@author: ghiggi
"""
import fsspec
from satpy import Scene
from satpy.readers import FSFile

from goes_api import find_latest_files

###---------------------------------------------------------------------------.
# Install satpy to execute this script
# conda install -c conda-forge satpy

###---------------------------------------------------------------------------.
#### Define protocol
base_dir = None

protocol = "gcs"
protocol = "s3"
fs_args = {}

###---------------------------------------------------------------------------.
#### Define satellite, sensor, product_level and product
satellite = "GOES-16"
sensor = "ABI"
product_level = "L1B"
product = "Rad"

###---------------------------------------------------------------------------.
#### Define sector and filtering options
sector = "F"
scene_abbr = None  # None download and find both locations
scan_modes = None  # select all scan modes (M3, M4, M6)
channels = None  # select all channels
channels = ["C01"]  # select channels subset
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

###---------------------------------------------------------------------------.
#### Open file from s3 using ffspec + satpy
fpaths = find_latest_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    connection_type="bucket",
)
# - Retrieve bucket url
fpaths = list(fpaths.values())[0]

# - Open files
files = fsspec.open_files(fpaths, anon=True)
type(files)  # fsspec.core.OpenFiles
type(files[0])  # fsspec.core.OpenFile
files[0].full_name

# - Define satpy FSFile
satpy_files = [FSFile(file) for file in files]

# - Use satpy
scn = Scene(filenames=satpy_files, reader="abi_l1b")
scn.available_dataset_names()
scn.available_composite_names()

# - Load channels
scn.load(scn.available_dataset_names())

####---------------------------------------------------------------------------.
#### Get pixel scan time array
from goes_api.abi_pixel_time import get_ABI_pixel_time

da_pixel_time = get_ABI_pixel_time(scn["C01"])
print(da_pixel_time)
