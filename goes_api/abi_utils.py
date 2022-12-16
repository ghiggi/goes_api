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
"""Define functions to retrieve ABI informations."""


def get_scan_mode_from_attrs(attrs):
    timeline_id = attrs['timeline_id']
    if timeline_id == "ABI Mode 3":
        scan_mode = "M3"
    elif timeline_id == "ABI Mode 4":
        scan_mode = "M4"
    elif timeline_id == "ABI Mode 6":
        scan_mode = "M6"
    else: 
        raise ValueError(f"'timeline_id' attribute not recognized. Value is {timeline_id}.")
    return scan_mode 


def get_sector_from_attrs(attrs):
    scene_id = attrs['scene_id']
    if scene_id == "Full Disk":
        sector = "F"
    elif scene_id == "CONUS":
        sector = "C"
    elif scene_id == "Mesoscale":
        sector = "M"
    else: 
        raise ValueError(f"'scene_id' attribute not recognized. Value is {scene_id}.")
    return sector 


def get_resolution_from_str(string): 
    if string == "0.5km at nadir":
        resolution = "500"
    elif string == "1km at nadir":
        resolution = "1000"
    elif string == "2km at nadir":
        resolution = "2000"
    elif string == "4km at nadir":
        resolution = "4000"
    elif string == "8km at nadir":
        resolution = "8000"
    elif string == "10km at nadir":
        resolution = "10000"
    else: 
        raise ValueError(f"'resolution' not recognized. Value is {string}.")
    return resolution 


def get_resolution_from_attrs(attrs):
    resolution = attrs['spatial_resolution']
    return get_resolution_from_str(resolution)

def get_abi_shape(sector, resolution): 
    resolution = int(resolution) 
    # Retrieve shape at 500 m 
    if sector == "F": 
        shape = (21696, 21696)
    elif sector == "C":
        shape = (6000, 10000)
    else: # sector == "M"
        shape = (2000, 2000)
    # Retrieve shape at reduced resolutions 
    reduction_factor = int(resolution/500)
    shape = tuple([int(pixels/reduction_factor) for pixels in shape])
    return shape 