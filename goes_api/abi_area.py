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
"""Define functions to retrieve ABI fixed grid AreaDefinition."""

# from pyresample import AreaDefinition
from pyresample_dev.utils_swath import * # temporary hack


def get_abi_shape(sector, resolution):
    """Returns the shape (height, width) of the ABI fixed grid."""
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


def get_abi_fixed_grid_extent(satellite, sector, resolution):
    """Returns the area extent of the ABI fixed grid."""
    if sector == "F":  
        area_extent = (-5434894.8851, -5434894.8851, 5434894.8851, 5434894.8851)
    elif sector == "C":
        if satellite.lower() == "goes-16":
            area_extent = (-3627271.2913, 1583173.6575, 1382771.9287, 4589199.5895)
        elif satellite.lower() in ["goes-17", "goes-18"]: 
            area_extent = (-2505021.61, 1583173.6575, 2505021.61, 4589199.5895)
        else: 
            raise NotImplementedError(f"No area extent available for {satellite} over CONUS domain.")
    else: 
        raise ValueError("For mesoscale sector, the area extent be retrieved from the data.")
    return area_extent


def get_abi_fixed_grid_projection(satellite):
    """Returns the projection dictionary of the ABI fixed grid."""
    if satellite.lower() == "goes-16":
        proj_dict = {
           "proj": "geos",
           "sweep": "x",
           "lon_0": -75,
           "h": 35786023,
           "x_0": 0,
           "y_0": 0,
           'units': 'm',
           "ellps": "GRS80",
           "no_defs": "null",
           "type": "crs",
       }
    elif satellite.lower() in ["goes-17", "goes-18"]:
        proj_dict = {
           "proj": "geos",
           "sweep": "x",
           "lon_0": -137,
           "h": 35786023,
           "x_0": 0,
           "y_0": 0,
           'units': 'm',
           "ellps": "GRS80",
           "no_defs": "null",
           "type": "crs",
       }
    else: 
        raise NotImplementedError(f"No projection available for {satellite}.")
    return proj_dict


def get_abi_fixed_grid_area(satellite, sector, resolution):
    """Returns the AreaDefinition of the ABI fixed grid. 
    
    https://github.com/pytroll/satpy/blob/main/satpy/readers/abi_base.py#L219
    """
    proj_dict = get_abi_fixed_grid_projection(satellite)
    extent = get_abi_fixed_grid_extent(satellite, sector, resolution)
    height, width = get_abi_shape(sector, resolution)
    name = f"{satellite} ABI {sector} at {resolution} m SSP resolution."
    # Define parameters 
    area_params = {} 
    area_params["proj_id"] = name
    area_params["area_id"] = name
    area_params["description"] = name
    area_params["projection"] = proj_dict
    area_params["width"] = width
    area_params["height"] = height  
    area_params["area_extent"] = extent
    # Define AreaDefinition 
    area_def = AreaDefinition(**area_params)
    return area_def      


# import cartopy.crs as ccrs
# import matplotlib.pyplot as plt

# proj_crs = ccrs.PlateCarree()
# fig, ax = plt.subplots(subplot_kw=dict(projection=proj_crs))
# area = get_abi_fixed_grid_area(satellite="goes-16", sector="F", resolution="2000")
# area.plot()

# proj_crs = ccrs.PlateCarree()
# fig, ax = plt.subplots(subplot_kw=dict(projection=proj_crs))
# area = get_abi_fixed_grid_area(satellite="goes-17", sector="F", resolution="2000")
# area.plot()

# # TODO: DOUBLE CHECK 
# proj_crs = ccrs.PlateCarree()
# fig, ax = plt.subplots(subplot_kw=dict(projection=proj_crs))
# area = get_abi_fixed_grid_area(satellite="goes-17", sector="C", resolution="2000")
# area.plot()

# proj_crs = ccrs.PlateCarree()
# fig, ax = plt.subplots(subplot_kw=dict(projection=proj_crs))
# area = get_abi_fixed_grid_area(satellite="goes-16", sector="C", resolution="2000")
# area.plot()