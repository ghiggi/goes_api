#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 10:08:08 2022

@author: ghiggi
"""
import datetime
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from goes_api import find_files
import requests
from io import BytesIO

def create_circular_mask(img, center=None, radius=None, radius_buffer=None):
    w, h = img.shape
    if center is None: # use the middle of the image
        center = (int(w/2), int(h/2))
    if radius is None: # use the smallest distance between the center and image walls
        radius = min(center[0], center[1], w-center[0], h-center[1])
    if radius_buffer is not None: 
        radius = radius + radius_buffer
        
    Y, X = np.ogrid[:h, :w]
    dist_from_center = np.sqrt((X - center[0])**2 + (Y-center[1])**2)

    mask = dist_from_center <= radius
    return mask

def retrieve_scan_index(ds):
    mask = create_circular_mask(img=ds['Rad'].data,
                                radius_buffer=-40)
    da = ds['DQF']
    da = da.fillna(-2)
    da = da.where(da<0, 5) # fill  >0 with 5 
    da = da.where(mask, -1) 
    
    # da.plot.imshow()
    
    diff_arr = np.abs(np.diff(da.data, axis=0))
    indices = np.where(np.any(diff_arr==7,axis=1))[0] + 1
    return indices

 
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
fpaths = sorted(fpaths_dict[datetime.datetime(2019, 7, 25, 15, 10)]) # TODO DEBUG SORTED AND TIME SUBSET
# - Select http url
fpath = fpaths[1]
print(fpath)

# - Open via bytesIO
resp = requests.get(fpath)
f_obj = BytesIO(resp.content)
ds = xr.open_dataset(f_obj)
print(ds.attrs['spatial_resolution'])
# ds["Rad"].plot.imshow()
# ds["DQF"].plot.imshow()
 
print(retrieve_scan_index(ds))

# 2 km 
# ds["Rad"].isel(y=slice(657-10, 657+10), x= slice(2250, 2300)).plot.imshow()

# 500 m 
# ds["Rad"].plot.imshow()
# ds["Rad"].isel(y=slice(2628-10, 2628+10), x=slice(12650, 12680)).plot.imshow()


####---------------------------------------------------------------------------.
#### Fig 4. (Gunshor et al., 2020)
start_time = datetime.datetime(2019, 9, 17, 9, 30)
end_time = datetime.datetime(2019,  9, 17, 9, 40)

fpaths_dict = find_files(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite="GOES-17",
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
fpaths = sorted(fpaths_dict[datetime.datetime(2019, 9, 17, 9, 30)]) # TODO DEBUG SORTED AND TIME SUBSET

# - Select http url
fpath = fpaths[0]
print(fpath)

# - Open via bytesIO
resp = requests.get(fpath)
f_obj = BytesIO(resp.content)
ds = xr.open_dataset(f_obj)
print(ds.attrs['spatial_resolution'])
# ds["Rad"].plot.imshow()
# ds["DQF"].plot.imshow()

indices = retrieve_scan_index(ds)
print(indices)
np.diff(indices)
print(retrieve_scan_index(ds))


####---------------------------------------------------------------------------.
#### GOES 16 

# C01  (1 km) (10848x10848)
# - first 20 avoid 
# - 808-1313   (505 spacing)
# - 1314

10847/808

# C02 (500 m)
# - 604-1615   (1012 spacing)
# - 1616-2627  (1012 spacing)
# - 2628

# C07 (2 km) 
# - 151-403    (252 spacing)
# - 404-656    (252 spacing)
# - 657-



#### GOES 17 

# C10 (2 km) (GOES-17) (5424x5424)
# - 934
# - 1188  
# - 1442 
# - 1696 
# - 1950
# - 2204

# - 2453
# - 2707
# - 2961

# C01  (1 km) (GOES-17) (10848x10848)



print(retrieve_scan_index(ds))




 