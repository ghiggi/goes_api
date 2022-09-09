#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  5 12:46:54 2022

@author: ghiggi
"""
import requests
import datetime
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
from goes_api import find_files
from goes_api.dev.dev_scan_time_code import *

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
# channels = ["C01","C02","C07","C13"]  # select channels subset
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

####---------------------------------------------------------------------------.
#### Fig 3. (Gunshor et al., 2020)
start_time = datetime.datetime(2019, 7, 25, 15, 10)
end_time = datetime.datetime(2019, 7, 25, 15, 21)

fpaths_dict = find_files(
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
    group_by_key = "start_time", 
    connection_type="https",
    verbose=True,
)
fpaths = sorted(fpaths_dict[datetime.datetime(2019, 7, 25, 15, 20)])  
# - Select http url
fpath = fpaths[7]
print(fpath)

# - Open via bytesIO
resp = requests.get(fpath)
f_obj = BytesIO(resp.content)
ds = xr.open_dataset(f_obj)


# ds["Rad"].plot.imshow()
ds["DQF"].plot.imshow()

disc_mask = ds['DQF'].data
disc_mask[np.isfinite(disc_mask)] = 1
# plt.imshow(disc_mask)
# np.unique(disc_mask)

###---------------------------------------------------------------------------.
### Plot scan modes 
scan_modes =  ["M6","M4","M3"]
satellites = ["G16", "G17"]
 
list_scan_modes = []
for satellite in satellites:
    for scan_mode in scan_modes:
        arr = get_time_offset_array(disc_mask, satellite = satellite, scan_mode=scan_mode)  
        da = xr.DataArray(arr,  dims=['y','x'])
        da.name = satellite + "-" + scan_mode
        list_scan_modes.append(da)
        
ds_scan_modes = xr.merge(list_scan_modes)       
ds_scan_modes.to_array("Scan Mode").plot.imshow(col="Scan Mode", col_wrap=3, cmap="Spectral", origin="upper")

###---------------------------------------------------------------------------.

diff_scan = ds_scan_modes["G16-M6"].diff(dim='y')
diff_scan.plot.imshow()
np.nanmax(diff_scan.data)

diff_scan = ds_scan_modes["G16-M3"].diff(dim='y')
diff_scan.plot.imshow()
np.nanmax(diff_scan.data)
plt.hist(diff_scan.data.flatten())
diff_scan = ds_scan_modes["G17-M6"].diff(dim='y')
diff_scan.plot.imshow()
np.nanmax(diff_scan.data) # 30 seconds max 

a = ds_scan_modes["G16-M6"] - ds_scan_modes["G17-M6"] 
a.plot.imshow(cmap="Spectral", origin="upper")

a = ds_scan_modes["G16-M6"] - ds_scan_modes["G16-M4"] 
a.plot.imshow(cmap="Spectral", origin="upper")

# a = ds_scan_modes["G16-M3"] - ds_scan_modes["G17-M3"] 
# a.plot.imshow(cmap="Spectral", origin="upper")


get_scan_duration()
start_time_offset, end_time_offset = get_scan_time_offsets("G16", "M6")
    
 











