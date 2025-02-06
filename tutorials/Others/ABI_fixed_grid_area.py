#!/usr/bin/env python3
"""
Created on Mon May  8 17:47:07 2023

@author: ghiggi
"""
import cartopy.crs as ccrs
import matplotlib.pyplot as plt

from goes_api.abi_area import get_abi_fixed_grid_area

#### GOES 16 FD
proj_crs = ccrs.PlateCarree()
fig, ax = plt.subplots(subplot_kw=dict(projection=proj_crs))
area = get_abi_fixed_grid_area(satellite="goes-16", sector="F", resolution="2000")
area.plot()

#### GOES 17 FD
proj_crs = ccrs.PlateCarree()
fig, ax = plt.subplots(subplot_kw=dict(projection=proj_crs))
area = get_abi_fixed_grid_area(satellite="goes-17", sector="F", resolution="2000")
area.plot()

#### GOES 17 PACUS
proj_crs = ccrs.PlateCarree()
fig, ax = plt.subplots(subplot_kw=dict(projection=proj_crs))
area = get_abi_fixed_grid_area(satellite="goes-17", sector="C", resolution="2000")
area.plot()

#### GOES 16 CONUS
proj_crs = ccrs.PlateCarree()
fig, ax = plt.subplots(subplot_kw=dict(projection=proj_crs))
area = get_abi_fixed_grid_area(satellite="goes-16", sector="C", resolution="2000")
area.plot()

# Notes
# - https://cimss.ssec.wisc.edu/satellite-blog/archives/26386/comment-page-1
