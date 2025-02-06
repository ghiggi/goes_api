#!/usr/bin/env python3
"""
Created on Thu Mar 24 14:33:48 2022

@author: ghiggi
"""
from io import BytesIO

import requests
import xarray as xr

from goes_api import find_latest_files

###---------------------------------------------------------------------------.
#### Define protocol
base_dir = None

protocol = "gcs"
protocol = "s3"
fs_args = {}

###---------------------------------------------------------------------------.
#### Define satellite, sensor, product_level and product
satellite = "GOES-16"
sensor = "SUVI"
product_level = "L1B"
product = "He303"  # Fe195

# ----------------------------------------------------------------------------.
#### Open file using in-memory buffering via https requests
fpaths = find_latest_files(
    look_ahead_minutes=30,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    connection_type="https",
)
# - Select http url
fpath = list(fpaths.values())[0][0]
print(fpath)

# - Open the dataset
resp = requests.get(fpath)
f_obj = BytesIO(resp.content)
ds = xr.open_dataset(f_obj)
ds.title
# ----------------------------------------------------------------------------.
#### Plotting
ds["RAD"].plot.imshow(vmin=2, vmax=80)

# Plotting on log scale
from matplotlib import colors

norm = colors.LogNorm(vmin=2, vmax=80)
ds["RAD"].plot.imshow(norm=norm)
