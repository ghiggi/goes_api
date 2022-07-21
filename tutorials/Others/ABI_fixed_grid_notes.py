#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 11:32:08 2022

@author: ghiggi
"""
####--------------------------------------------------------------------------.
#### Define GEOS AreaDefinition from fixed position 
# - GOES16 definition took from https://github.com/pytroll/satpy/blob/main/satpy/etc/areas.yaml#L443
# - The area extent differ slightly from the AreaDef derived from the ABI files !!! 
# - area_extent represents the edge of pixels
import satpy 
from pyresample.area_config import create_area_def
from pyresample import AreaDefinition

# goes_east_abi_f_1km
area_params = {} 
area_params["area_id"] = "dummy"
area_params["description"] =  "GOES East ABI Full Disk at 1 km SSP resolution"
area_params["projection"] = {
    "proj": "geos",
    "sweep": "x",
    "lon_0": -75,
    "h": 35786023,
    "x_0": 0,
    "y_0": 0,
    "ellps": "GRS80",
    "no_defs": "null",
    "type": "crs",
}
# Optional --> DynamicAreaDefinition
area_params["shape"] = [10848, 10848]
area_params["area_extent"] = [-5434894.885056, -5434894.885056, 5434894.885056, 5434894.885056]
 
#### - Option 1
area_def = create_area_def(**area_params)
area_def.shape

#### - Option 2
area_def_kwargs = area_params.copy() 
area_def_kwargs['proj_id'] = "dummy"
area_def_kwargs['width'] = area_params["shape"][0]
area_def_kwargs['height'] = area_params["shape"][1]
_ = area_def_kwargs.pop("shape")
area_def1 = AreaDefinition(**area_def_kwargs) 

#### - Option 3
area_def2 = satpy.resample.get_area_def("goes_west_abi_f_1km") 
area_def == area_def1
area_def == copy.deepcopy(area_def)
area_def.shape == area_def1.shape
area_def.area_extent == area_def1.area_extent
area_def.resolution
area_def1.resolution
area_def.rotation
area_def1.rotation
np.testing.assert_equal(area_def.get_proj_coords()[0], area_def1.get_proj_coords()[0])
np.testing.assert_equal(area_def.get_proj_coords()[1], area_def1.get_proj_coords()[1])

####--------------------------------------------------------------------------.
#### Define GEOS AreaDefinition from file
# - From # https://github.com/pytroll/satpy/blob/main/satpy/readers/abi_base.py
# - Note: GOES ABI L1B data are remapped on the ABI fixed grid 
#         Hence the projection dictionary does not vary with time !!! 
import os 
import numpy as np
import xarray as xr 
from pyresample import AreaDefinition
dir_path = "/media/ghiggi/New Volume/Data/GOES16/ABI_L1b-RadF/2020/187/11"
fname = "OR_ABI-L1b-RadF-M6C01_G16_s20201871120222_e20201871129530_c20201871129587.nc"
fpath = os.path.join(dir_path, fname)
ds = xr.open_dataset(fpath)

# Get projection information 
proj_dict = ds["goes_imager_projection"].attrs
lon_0 = proj_dict['longitude_of_projection_origin']
sweep_axis = proj_dict['sweep_angle_axis']
a = proj_dict['semi_major_axis']
b = proj_dict['semi_minor_axis']
h = proj_dict['perspective_point_height']
h = np.float64(h)

# Get x and y grid projection coordinates in scan angle radians
# - Scan angles increases the further away the pixel is from the satellite nadir.
x = ds['x'].astype("float64")  
y = ds['y'].astype("float64")  

# Get x and y geos projection coordinates in m 
# - https://proj.org/operations/projections/geos.html 
x_proj = x*h 
y_proj = y*h

# Get shape of the ABI fixed grid 
nlines = len(y)
ncols = len(x) 

# Get the area extent in rad [x1, y1, x2, y2]
# - area_extent represents the edge of pixels !!!
x_l = x[0].values
x_r = x[-1].values
y_l = y[-1].values
y_u = y[0].values
x_half = (x_r - x_l) / (ncols - 1) / 2.
y_half = (y_u - y_l) / (nlines - 1) / 2.
area_extent = (x_l - x_half, y_l - y_half, x_r + x_half, y_u + y_half)
print(area_extent)
# Get the area extent in meters
area_extent = tuple(np.round(h * val, 6) for val in area_extent)
area_extent = np.asarray(area_extent)
print(area_extent)
proj_dict = {'proj': 'geos',
             'lon_0': float(lon_0),
             'a': float(a),
             'b': float(b),
             'h': h,
             'units': 'm',
             'sweep': sweep_axis}

# Define AreaDefinition 
area_id = ds.attrs.get('orbital_slot', 'abi_geos')
description =  ds.attrs.get('spatial_resolution', 'ABI file area')
proj_id =  'abi_fixed_grid'
area_def = AreaDefinition(
    area_id,
    description,
    proj_id,
    proj_dict,
    ncols,
    nlines,
    area_extent,
)

lons, lats = area_def.get_lonlats()
lons[~np.isfinite(lons)] = np.nan
lats[~np.isfinite(lats)] = np.nan

# Compared the area extent with the AreaDef prescribed in areas.yaml 
print(area_def.area_extent)
print(area_def1.area_extent)

####--------------------------------------------------------------------------.
#### Define GEOS pyproj 
# - h = viewing point (satellite position) height above the earth
# - GOES satellite: E/W axis (x) as sweep angle 
# - Meteosat satellite: N/S axis (y) as sweep angle
import os
import xarray as xr 
from pyproj import CRS
from pyproj import Proj

dir_path = "/media/ghiggi/New Volume/Data/GOES16/ABI_L1b-RadF/2020/187/11"
fname = "OR_ABI-L1b-RadF-M6C01_G16_s20201871120222_e20201871129530_c20201871129587.nc"
fpath = os.path.join(dir_path, fname)
ds = xr.open_dataset(fpath)
proj_dict = ds["goes_imager_projection"].attrs

#### - Option 1
a = proj_dict['semi_major_axis']
b = proj_dict['semi_minor_axis']
h = proj_dict['perspective_point_height']
lon_0 = proj_dict['longitude_of_projection_origin']
sweep_axis = proj_dict['sweep_angle_axis']

p = Proj(proj='geos', h=h, lon_0=lon_0, sweep=sweep_axis, ellps="GRS80")
crs = p.crs

#### - Option 2
crs1 = CRS.from_cf(proj_dict)

print(crs)
print(crs1)

print(crs.to_proj4())
print(crs1.to_proj4())

print(crs.srs)
print(crs1.srs)

print(crs.to_wkt())
print(crs1.to_wkt())
 
####--------------------------------------------------------------------------.
#### Get L1B ABI lat/lons arrays
# Docs:
# https://proj.org/operations/projections/geos.html
# https://www.goes-r.gov/users/docs/PUG-L1b-vol3.pdf
# https://www.goes-r.gov/downloads/resources/documents/GOES-RSeriesDataBook.pdf
# https://makersportal.com/blog/2018/11/25/goes-r-satellite-latitude-and-longitude-grid-projection-algorithm

import os
import numpy as np 
import xarray as xr 
import matplotlib.pyplot as plt 
from pyproj import CRS
from pyproj import Proj

dir_path = "/media/ghiggi/New Volume/Data/GOES16/ABI_L1b-RadF/2020/187/11"
fname = "OR_ABI-L1b-RadF-M6C01_G16_s20201871120222_e20201871129530_c20201871129587.nc"
fpath = os.path.join(dir_path, fname)
ds = xr.open_dataset(fpath)

#### - Option 1 (with user-made formulas)
def get_ABI_L1B_lats_lons(ds):
    # Get projection information 
    proj_dict = ds['goes_imager_projection'].attrs
    
    # Get longitude of geos projection origin
    lon_0 = proj_dict["longitude_of_projection_origin"]
            
    # Get the radius of Earth at the equator 
    r_eq = proj_dict["semi_major_axis"]
    
    # Get the radius of Earth at the pole 
    r_pol = proj_dict["semi_minor_axis"]
    
    # Get perspective point height
    h =  proj_dict["perspective_point_height"]
    
    # Get the distance from satellite to Earth center 
    H = h + r_eq

    # Retrieve 1D scan angle vector in radians
    x = ds['x'].values.astype("float64") # IMPORTANT TO USE FLOAT64 !!!
    y = ds['y'].values.astype("float64") # IMPORTANT TO USE FLOAT64 !!!
    
    # Create a meshgrid filled with scan angle in radians
    xx, yy = np.meshgrid(x,y)
    
    # Convert scan angles to geodetic latitude and longitude
    lambda_0 = (lon_0*np.pi)/180.0
    
    a_var = np.sin(xx)**2 + (np.cos(xx)**2)*(np.cos(yy)**2 + (((r_eq**2)/(r_pol**2))*(np.sin(yy)**2)))
    b_var = -2.0*H*np.cos(xx)*np.cos(yy)
    c_var = (H**2.0)-(r_eq**2.0)
    
    r_s = (-1.0*b_var - np.sqrt((b_var**2)-(4.0*a_var*c_var)))/(2.0*a_var)
    
    s_x = r_s*np.cos(xx)*np.cos(yy)
    s_y = - r_s*np.sin(xx)
    s_z = r_s*np.cos(xx)*np.sin(yy)
    
    lat = (180.0/np.pi)*(np.arctan(((r_eq**2)/(r_pol**2))*((s_z/np.sqrt(((H-s_x)**2)+(s_y**2))))))
    lon = (lambda_0 - np.arctan(s_y/(H-s_x)))*(180.0/np.pi)
    # Return lat lon 
    return lat, lon
    
lat, lon = get_ABI_L1B_lats_lons(ds)

plt.imshow(lat); plt.colorbar(); plt.show()
plt.imshow(lon); plt.colorbar(); plt.show()

#------------------------------------------------------.
#### - Option 2 (with pyproj)
# - Note: pyproj returns Inf that must be masked to nan
proj_dict = ds['goes_imager_projection'].attrs
sweep_axis = proj_dict["sweep_angle_axis"]
lon_0 = proj_dict["longitude_of_projection_origin"]
h =  proj_dict["perspective_point_height"]

# Convert x and y scan angle from radians to meters 
x = ds['x'].values.astype("float64")*h # IMPORTANT TO USE FLOAT64 !!!
y = ds['y'].values.astype("float64")*h # IMPORTANT TO USE FLOAT64 !!!
xx, yy = np.meshgrid(x, y)


p = Proj(proj='geos', h=h, lon_0=lon_0, sweep=sweep_axis, ellps="GRS80")

lons, lats = p(xx, yy,inverse=True)
lons[~np.isfinite(lons)] = np.nan
lats[~np.isfinite(lats)] = np.nan
 
plt.imshow(lats); plt.colorbar(); plt.show()
plt.imshow(lons); plt.colorbar(); plt.show()

### - Option 3 (with pyproj)
crs = CRS.from_cf(ds['goes_imager_projection'].attrs)
p = Proj(crs)
lons1, lats1 = p(xx, yy, inverse=True)
lons1[~np.isfinite(lons1)] = np.nan
lats1[~np.isfinite(lats1)] = np.nan
 
np.testing.assert_equal(lons, lons1) 
np.testing.assert_equal(lats, lats1) 

### Show regions where mismatch occurs between using pyproj and manual lat/lon computation 
# - If x and y are not converted to float64, the difference at the border can reach up to 0.6°!!! 
# np.testing.assert_equal(lons, lon) 
# np.testing.assert_equal(lats, lat) 

# plt.imshow(lats-lat); plt.colorbar(); plt.show()
# plt.imshow(lons-lon); plt.colorbar(); plt.show()

# diff = np.abs(lons-lon)
# diff = np.abs(lats-lat)

# thr = 0.00001 # 0.01 # 0.1
# lat_coord = lats[diff > thr]
# lon_coord = lons[diff > thr]
# plt.scatter(lon_coord, lat_coord)

####--------------------------------------------------------------------------
#### Get pixel center lat/lons coordinates of outer valid pixels
np.nanmin(lons)
np.nanmax(lons)
np.nanmin(lats)
np.nanmax(lats)

####--------------------------------------------------------------------------.
#### ABI file EDA 
import os
import numpy as np 
import xarray as xr 
import matplotlib.pyplot as plt 
from pyproj import CRS
from pyproj import Proj

dir_path = "/media/ghiggi/New Volume/Data/GOES16/ABI_L1b-RadF/2020/187/11"
fname = "OR_ABI-L1b-RadF-M6C01_G16_s20201871120222_e20201871129530_c20201871129587.nc"
fpath = os.path.join(dir_path, fname)
ds = xr.open_dataset(fpath)


print(ds.coords)
print(ds.data_vars)
print(ds.attrs)

# Get x and y grid projection coordinates in scan angle radians
# - Scan angles increases the further away the pixel is from the satellite nadir.
x = ds['x'].astype("float64")  
y = ds['y'].astype("float64")  
 
#### - ABI time aquisition information 
ds['t'].values # Time at midpoint 
ds['time_bounds'].values # Start and end time 
ds.attrs["time_coverage_start"]
ds.attrs["time_coverage_end"]

#### - Scan angle interval across x and y 
# At 0.5 km res -->  0.000014 rad --> 14 urad
# At 1 km res   -->  0.000028 rad --> 28 urad
# At 2 km res   -->  0.000056 rad --> 54 urad
# At 4 km res   -->  0.000108 rad --> 108 urad
# At 10 km res  -->  0.00028  rad --> 280 urad

np.unique(np.diff(x.values), return_counts=True)
np.unique(np.diff(y.values), return_counts=True)

#### - Angular diameter in rad
# --> Differ from PUG L1B values at the 3 decimal digit !
# E/W coverage: 0.303744003895165 rad
print(x[-1].values - x[0].values + x_half*2)

# N/S coverage: 0.303744003895165 rad  
print(y[0].values - y[-1].values + y_half*2)

##--------------------------------------------------- 
#### - Scan angle coordinates 
# x is increasing (left to right)
# y is decreasing (top to bottom) 

# At nadir 
x.shape
x[5423]
x[5424]

y[5424]
y[5423]

##--------------------------------------------------- 
#### - Approximate area_extent and lat/lon corners  
# - The scan angle in radians is an approximation to 6 decimal digits
# - When it is multiplied by h to convert to m, it cause a slight difference ! 

print(ds['x_image_bounds']) 
print(ds['y_image_bounds']) 
x1 = ds['x_image_bounds'][0].values.astype("float64") * h
x2 = ds['x_image_bounds'][1].values.astype("float64") * h
y1 = ds['y_image_bounds'][1].values.astype("float64") * h
y2 = ds['y_image_bounds'][0].values.astype("float64") * h 
area_extent_bounds = np.asarray([x1,y1, x2, y2])
area_extent_bounds - area_extent

##--------------------------------------------------- 
#### - Get corner lat/lons coordinates of outer valid pixels
ds["geospatial_lat_lon_extent"]

ds["geospatial_lat_lon_extent"].attrs["geospatial_westbound_longitude"]
ds["geospatial_lat_lon_extent"].attrs["geospatial_eastbound_longitude"]

ds["geospatial_lat_lon_extent"].attrs["geospatial_northbound_latitude"]
ds["geospatial_lat_lon_extent"].attrs["geospatial_southbound_latitude"]

####--------------------------------------------------------------------------.