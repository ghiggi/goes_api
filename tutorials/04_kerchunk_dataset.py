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

import fsspec
import ujson
import datetime
import xarray as xr
from goes_api import find_files, generate_kerchunk_files

from dask.distributed import Client

client = Client(n_workers=20)
client

###---------------------------------------------------------------------------.
#### Define protocol and local directory
reference_dir = "/ltenas3/0_Data/kerchunk_json/"
protocol = "s3"  # "gcs"

###---------------------------------------------------------------------------.
#### Define satellite, sensor, product_level and product
satellite = "GOES-16"
sensor = "ABI"
product_level = "L1B"
product = "Rad"

###---------------------------------------------------------------------------.
#### Define sector and filtering options
sector = "F"

scene_abbr = None  # M1, M2
scan_modes = None  # M3, M6
channels = None  # ['C01', 'C02', 'C15']

filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

start_time = datetime.datetime(2020, 7, 5, 11, 30)
end_time = datetime.datetime(2020, 7, 5, 11, 40)

###---------------------------------------------------------------------------.
################################
#### Create Reference JSON  ####
################################
fs_args = {
    # "mode": "rb",
    "anon": True,
    "default_fill_cache": False,
    "default_cache_type": "none",
}

generate_kerchunk_files(
    reference_dir=reference_dir,
    n_processes=20,
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
    progress_bar=True,
)

###---------------------------------------------------------------------------.
##################################
#### Retrieve Reference Files ####
##################################
fpaths = find_files(
    base_dir=reference_dir,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    group_by_key=None,
    verbose=True,
)

####--------------------------------------------------------------------------.
#### - Open a file using kerchunk reference file
reference_fpath = fpaths[0]
with open(reference_fpath) as f:
    reference_dict = ujson.load(f)
reference_dict
# Option 1
m = fsspec.get_mapper(
    "reference://",
    fo=reference_fpath,
    remote_protocol="s3",
    remote_options={"anon": True},
)
# Option 2
fs = fsspec.filesystem(
    "reference",
    fo=reference_fpath,
    remote_protocol="s3",
    remote_options={"anon": True},
    skip_instance_cache=True,
)
m = fs.get_mapper("")

# Option 3
m = fsspec.get_mapper(
    "reference://",
    fo=reference_dict,
    remote_protocol="s3",
    remote_options={"anon": True},
)

# Read dataset (Option 1)
ds = xr.open_dataset(m, engine="zarr", consolidated=False)

# Read dataset (Option 2)
ds = xr.open_zarr(m, consolidated=False)

####--------------------------------------------------------------------------.
#### - Generate list of reference mappers
from goes_api.kerchunk import get_reference_mappers

m_list = get_reference_mappers(fpaths, protocol=protocol)
print(m_list)
ds = xr.open_dataset(m_list[0], engine="zarr", consolidated=False)

####--------------------------------------------------------------------------.
#### - Dig into reference file
fs = fsspec.filesystem(
    "reference",
    fo=reference_fpath,
    remote_protocol="s3",
    remote_options={"anon": True},
    skip_instance_cache=True,
)
# List netcdf array
fs.ls("", False)

# Look into global attributes
print(fs.cat(".zattrs").decode())
print(fs.cat(".zgroup").decode())

# Look into Radiance array
fs.ls("Rad", False)[:10]
print(fs.cat("Rad/.zattrs").decode())
print(fs.cat("Rad/.zarray").decode())
print(fs.cat("Rad/0.16"))

fs.references["Rad/0.16"]

####--------------------------------------------------------------------------.
