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

import datetime
from goes_api import find_files
    

###---------------------------------------------------------------------------.
#### Define protocol or local directory
# - If base_dir specified
# --> Search on local storage
# --> Protocol must be None (or "file" ... see ffspec.LocalFileSystem)

# - If protocol is specified
# --> base_dir must be None
# --> Search on cloud bucket

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

import time
t_i = time.time()
###---------------------------------------------------------------------------.
#### Define sector and filtering options
start_time = datetime.datetime(2019, 7, 17, 11, 30)
end_time = datetime.datetime(2020, 1, 17, 11, 40)

sector = "F"
scan_modes = None   # select all scan modes (M3, M4, M6)
channels = ["C01"]  # select channels subset
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels

####---------------------------------------------------------------------------.
#### Find files between start_time and end_time
fpaths = find_files(
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
    verbose=True,
)

####---------------------------------------------------------------------------.
t_f = time.time()
print(round(t_f - t_i,2)) # 12 min for 6 months queries

import numpy as np
from goes_api.info import get_key_from_filepaths
 
# Investigate system environment 
list_se = np.array(get_key_from_filepaths(fpaths, "system_environment"), dtype=str)
idx_not_or = np.where(list_se != "OR")

# Investigate scan modes 
list_scan_mode = np.array(get_key_from_filepaths(fpaths, "scan_mode"), dtype=str)
np.unique(list_scan_mode, return_counts=True)

idx_m4 = np.where(list_scan_mode == "M4")
idx_m3 = np.where(list_scan_mode == "M3")

np.array(fpaths)[idx_m4]

# M4 scan 
fpaths = np.array(['s3://noaa-goes16/ABI-L1b-RadF/2019/265/16/OR_ABI-L1b-RadF-M4C01_G16_s20192651605225_e20192651610028_c20192651610095.nc',
                   's3://noaa-goes16/ABI-L1b-RadF/2019/355/16/OR_ABI-L1b-RadF-M4C01_G16_s20193551605173_e20193551609576_c20193551610043.nc',
                   's3://noaa-goes16/ABI-L1b-RadF/2019/356/11/OR_ABI-L1b-RadF-M4C01_G16_s20193561105224_e20193561110027_c20193561110093.nc'])





####---------------------------------------------------------------------------.
