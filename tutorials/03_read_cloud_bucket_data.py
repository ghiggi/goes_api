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

import xarray as xr

from goes_api import find_latest_files

# NOTE: For GOES performance benchmarks, check https://github.com/ghiggi/goes_benchmarks/
# To read data using kerchunk referene file, see tutorial 04_kerchunk_*.py

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
sector = "M"
scene_abbr = ["M1"]  # None download and find both locations
scan_modes = None  # select all scan modes (M3, M4, M6)
channels = None  # select all channels
channels = ["C01"]  # select channels subset
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

####---------------------------------------------------------------------------.
#### Open file using in-memory buffering via https requests and bytesIO
fpaths = find_latest_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    connection_type="https",
)
# - Select http url
fpath = list(fpaths.values())[0][0]
print(fpath)

# - Open via bytesIO
from io import BytesIO

import requests

resp = requests.get(fpath)
f_obj = BytesIO(resp.content)
ds = xr.open_dataset(f_obj)
ds["Rad"].plot.imshow()


####---------------------------------------------------------------------------.
#### Open file using in-memory buffering via https requests and netCDF4
fpaths = find_latest_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    connection_type="https",
)
# - Select http url
fpath = list(fpaths.values())[0][0]
print(fpath)

# - Open using netCDF4
import netCDF4
import requests

resp = requests.get(fpath)
nc_ds = netCDF4.Dataset("whatevername", memory=resp.content)
xr_bk = xr.backends.NetCDF4DataStore(nc_ds)
ds = xr.open_dataset(xr_bk)
ds["Rad"].plot.imshow()

####---------------------------------------------------------------------------.
#### Open file using netCDF byte range requests
# - Perform byte range requests instead of downloading the whole file
# - This is quite slow !!!
# - Require: conda install -c conda-forge "libnetcdf>4.7.4=*_105
# - Other info: https://twitter.com/dopplershift/status/1286415993347047425

fpaths = find_latest_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    connection_type="nc_bytes",
)
# - Retrieve http url with #mode=bytes suffix
fpath = list(fpaths.values())[0][0]
print(fpath)

# - Open via netcdf #mode=bytes
ds = xr.open_dataset(fpath, engine="netcdf4")
ds["Rad"].plot.imshow()

####---------------------------------------------------------------------------.
#### Open file from s3 using ffspec
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
fpath = list(fpaths.values())[0][0]
print(fpath)

# - Open via fsspec
# --> This option do not close connection to the bucket
import fsspec

fs = fsspec.filesystem("s3", anon=True)
ds = xr.open_dataset(fs.open(fpath), engine="h5netcdf")
ds["Rad"].plot.imshow()

# - Alternative 1 with in-memory buffer data (and close connection)
from io import BytesIO

import fsspec

fs = fsspec.filesystem("s3", anon=True)
with fs.open(fpath, "rb") as f:
    ds = xr.open_dataset(BytesIO(f.read()), engine="h5netcdf")
ds["Rad"].plot.imshow()

# - Alternative 2 with in-memory buffer data (and close connection)
from io import BytesIO

import fsspec

with fsspec.open(fpath, mode="rb", anon=True) as f:
    ds = xr.open_dataset(BytesIO(f.read()), engine="h5netcdf")
ds["Rad"].plot.imshow()

####---------------------------------------------------------------------------.
#### Open file from s3 using ffspec and block-cache
# - blockcache just download/read the required data !!!
import appdirs
import fsspec.implementations.cached

cachedir = appdirs.user_cache_dir("ABI-block-cache")
storage_options = {"anon": True}
fs_s3 = fsspec.filesystem(protocol="s3", **storage_options)

# Block cache
fs_block = fsspec.implementations.cached.CachingFileSystem(
    fs=fs_s3,
    cache_storage=cachedir,
    cache_check=600,
    check_files=False,
    expiry_times=False,
    same_names=False,
)

with fs_block.open(fpath, block_size=2**20) as f:
    ds = xr.open_dataset(f, engine="h5netcdf", chunks="auto")
    print(ds["Rad"].data)
    ds["Rad"].plot.imshow()

del ds, f  # GOOD PRACTICE TO REMOVE, SINCE CONNECTION HAS BEEN CLOSED !

####---------------------------------------------------------------------------.
## Note of caution:
# - When processing files on-disk and passing a filename to xarray.open_dataset,
#  the autoclose=True argument (true by default) ensures files are closed when not needed.
# - When passing open files to xarray, xarray doesn't close them
# (which is very reasonable of xarray, because it can't reopen them).
# - It is user responsibility to close files and reopening them as needed.
# - Without closing the files, the max number of open files per process (i.e. 1024)
#  can be quickly exhausted.

###---------------------------------------------------------------------------.
