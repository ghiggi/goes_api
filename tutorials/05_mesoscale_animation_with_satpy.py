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

import fsspec
from dask.diagnostics import ProgressBar
from satpy import MultiScene, Scene
from satpy.readers import FSFile

from goes_api import find_latest_files

###---------------------------------------------------------------------------.
# Install satpy and imageio-ffmpeg to execute this script
# conda install -c conda-forge satpy imageio-ffmpeg

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
channels = ["C01", "C02", "C03"]  # select channels for True Color generation
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

###---------------------------------------------------------------------------.
#### Open files from s3 using ffspec + satpy
fpaths_dict = find_latest_files(
    N=10,
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

# List timesteps available
list(fpaths_dict.keys())

# Define scn for each timestep
scn_dict = {}
for timestep, fpaths in fpaths_dict.items():

    # - Open files
    files = fsspec.open_files(fpaths, anon=True)
    # files = fsspec.open_files(fpaths, s3={"anon": True})

    # - Define satpy FSFile
    satpy_files = [FSFile(file) for file in files]

    # - Use satpy
    scn = Scene(filenames=satpy_files, reader="abi_l1b")

    # - Load the channels
    scn.load(["C01", "C02", "C03"])

    # - Resample chanels to common grid (default the highest resolution)
    scn = scn.resample(scn.finest_area(), resampler="native")

    # - Create the composite
    scn.load(["true_color"])

    # - Add to dictionary
    scn_dict[timestep] = scn

print("Number of Scenes: ", len(scn_dict))

###---------------------------------------------------------------------------.
#### Generate an animation using satpy Multiscene functionalities
# - Create MultiScene
mscn = MultiScene(scn_dict.values())

# - Create the animation
with ProgressBar():
    mscn.save_animation(
        filename="/tmp/{name}_{start_time:%Y%m%d_%H%M%S}.mp4",
        # dataset=['true_color'], # THIS CAUSE A SATPY BUG
        fps=5,
    )  # batch_size=4)

###---------------------------------------------------------------------------.
