#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 10:17:32 2020

@author: ghiggi
"""

import s3fs
import numpy as np
import tqdm
import netCDF4 
import xarray as xr
import matplotlib.pyplot as plt
import cartopy
import cartopy.crs as ccrs
## noaa aws
## gcp public data goes 

# https://github.com/mnichol3/goesaws

# https://noaa-himawari8.s3.amazonaws.com/index.html
# https://console.cloud.google.com/storage/browser/gcp-public-data-goes-16;tab=objects?pli=1&prefix=&forceOnObjectsSortingFiltering=false
 
# conda install -c conda-forge "libnetcdf=4.7.4=*_105
## https://twitter.com/dopplershift/status/1286415993347047425

## Downalod from GCP 
gcsf PACKAGE
https://gcsfs.readthedocs.io/en/latest/index.html 

##----------------------------------------------------------------------------.
## List AWS GOES-16 netCDF data
# Use the anonymous credentials to access public data
fs = s3fs.S3FileSystem(anon=True)

# List contents of GOES-16 bucket.
fs.ls('s3://noaa-goes16/')
fs.ls('s3://noaa-goes16/ABI-L1b-RadF')
fs.ls('s3://noaa-goes17/ABI-L1b-RadF')
## Raw data 
# ABI-L1b-RadC (CONUS: USA view, every 5 mins)
# ABI-L1b-RadF (Full Disk: every 10 mins)
# ABI-L1b-RadM (Mesoscale interest area: Every 1 min).
#@ L2 products 
# RRQPE 
# TPW
# ACHA # CTH 
# ACM  # Cloud Mask 
# ACTP # Cloud Top Phase
# /year/dayofyear/hour/

files = np.array(fs.ls('noaa-goes16/ABI-L1b-RadF/2019/244/20/'))

# Channel 1 is blue
# Channel 2 is red
# Channel 3 is green. 
# --> Use red for best visable images. channel 10 is IR.


# https://developers.google.com/earth-engine/datasets/catalog/NOAA_GOES_17_MCMIPC TRUE COLOR
## True color 
##----------------------------------------------------------------------------.
res = [i for i in files if 'OR_ABI-L1b-RadF-M6C02' in i] 
res.sort()
res
##----------------------------------------------------------------------------.
# Download 
savedir = '/home/ghiggi/tmp/GOES16'
for i in tqdm.tqdm(range(len(res))):
    fs.get(res[i], savedir+res[i].split('/')[-1])
##----------------------------------------------------------------------------.  
## Direct access over HTTP  

server_path = 'http://noaa-goes16.s3.amazonaws.com'
product = 'ABI-L1b-RadF'
year = '2019'
doy = '244'
hour = '20' 
filename = 'OR_ABI-L1b-RadF-M6C02_G16_s20192442010192_e20192442019500_c20192442019543.nc'
url = server_path + "/" + product + "/" + year + "/" + doy + "/" + hour + "/"+ filename + "#mode=bytes"

## Load with netCDF4
data =  netCDF4.Dataset(url)
data.title

## Load with xarray 
ds = xr.open_dataset(url)                # no lazy loading 
ds = xr.open_dataset(url, chunks='auto') # lazy loading

ds
ds.attrs['title']
ds.attrs['spatial_resolution']

ds.nominal_satellite_height
ds.x_image_bounds
ds.y_image_bounds
 
#----------------------------------------------------------------------------.

# Load netCDF
filename = savedir + '/OR_ABI-L1b-RadF-M6C02_G16_s20192442010192_e20192442019500_c20192442019543.nc'
data = netCDF4.Dataset(filename,'r')

rad = data['Rad'][:]

# rad = np.ma.masked_where(rad > 7.5,rad)

# Satellite height
sat_h = data.variables['goes_imager_projection'].perspective_point_height

# Satellite longitude
sat_lon = data.variables['goes_imager_projection'].longitude_of_projection_origin

# Satellite sweep
sat_sweep = data.variables['goes_imager_projection'].sweep_angle_axis

# The projection x and y coordinates equals
# the scanning angle (in radians) multiplied by the satellite height (http://proj4.org/projections/geos.html)
X = data.variables['x'][:] * sat_h
Y = data.variables['y'][:] * sat_h

#-----------------------------------------------------------------------------.

#make figure
fig = plt.figure(figsize=(10, 10))
#add the map
ax = fig.add_subplot(1, 1, 1,projection=ccrs.Geostationary(satellite_height=sat_h,central_longitude=sat_lon,sweep_axis='x'))

#add coastlines
ax.add_feature(cartopy.feature.COASTLINE,zorder=1,color='w',lw=1)


#plot the image
pm = ax.pcolorfast(X,Y,rad,cmap='Greys_r',zorder=0,vmin=10)
ax.set_title('GOES-16',fontweight='bold',fontsize=14)

cbar = plt.colorbar(pm,ax=ax,shrink=0.75)
cbar.set_label('Radiance',fontsize=14)
cbar.ax.tick_params(labelsize=12)

plt.tight_layout()


import os
from satpy import Scene, MultiScene
from glob import glob
import datetime
from dask.diagnostics import ProgressBar




# I first find all of the channel 1 (C01) files for all times I want. 
# I then take the C01 filename and globify it to match all other channels for that time.
# Load all of the channels (C01-C16) 
# Resample/Subset/Aggregate 
# 2.5 hours for the 14 hours of data

Datetime = datetime.datetime(int(year),1,1,int(hour))
Datetime = Datetime + datetime.timedelta(days=int(doy))

start_time = datetime.datetime(year)
base_dir = server_path + "/" + product + "/" + year + "/" + doy + "/" + hour  
base_dir = server_path + "/" + product + '/{0:%Y}/{0:%j}'

base_dir =  savedir + '/{0:%Y}/{0:%Y_%m_%d_%j}
# Define channel names 
channels_names = ['C{:02d}'.format(x) for x in range(1, 17)]






 

scn = Scene(
    filenames=glob.glob("/path/to/the/Goes-16/data/*RadF*"),
    reader='abi_l1b'
)

channels = ['C{channel:02d}'.format(channel=chn) for chn in range(1, 17)]
scn.load(channels)

dt = Datetime
base_dir = base_dir.format(dt)
os.path.join(base_dir, 'OR_ABI-L1b-RadF-M3C01_G16_s{:%Y%j}*.nc').format(dt)
    
filepath = 'http://noaa-goes16.s3.amazonaws.com/ABI-L1b-RadF/2019/245/20/OR_ABI-L1b-RadF-M6C11_G16_s20192452020188_e20192452029496_c20192452029546.nc#mode=bytes'
scn = Scene(reader='abi_l1b', filenames=[filepath])
scn.load(['C11']) 
scn.show('C11')

scn.to_xarray_dataset()

# Download a folder (which is one time stamp) and contains all 16 channels and 10 segments
fs.get('noaa-himawari8/AHI-L1b-FLDK/2020/09/04/2350/*', 'data')
filenames = glob.glob('data/*')

# Himawari 
scn = Scene(reader='ahi_hsd', filenames=filenames)
scn.available_dataset_names(composites=True)
scn.load(['B04'])
scn.show('B04')



   
# 1200Z to 2359
## Retrieve available files on AWS/GCP servers 
c01_files = sorted(glob((os.path.join(base_dir, 'OR_ABI-L1b-RadF-M3C01_G16_s{:%Y%j}*.nc').format(dt))))
c01_files = [c01_file + "#mode=bytes" for c01_file in c01_files]

for c01_file in c01_files:
    ctime_idx = c01_file.find('e{:%Y}'.format(dt))
    all_files = glob(c01_file.replace('C01', 'C??')[:ctime_idx] + '*.nc')
    assert len(all_files) == 16
    
    scn = Scene(reader='abi_l1b', filenames=all_files)
    scn.load(ds_names)
    new_scn = scn.resample(scn.min_area(), resampler='native')
    yield new_scn

# https://satpy.readthedocs.io/en/stable/_modules/satpy/readers/abi_base.html#NC_ABI_BASE      
# https://satpy.readthedocs.io/en/stable/_modules/satpy/readers/abi_l1b.html#NC_ABI_L1B
# https://satpy.readthedocs.io/en/stable/_modules/satpy/readers/abi_l2_nc.html#NC_ABI_L2

        
# https://github.com/pytroll/pytroll-examples/blob/master/satpy/HRIT%20AHI%2C%20Hurricane%20Trami.ipynb
# https://github.com/pytroll/pytroll-examples/blob/master/satpy/ahi_true_color_pyspectral.ipynb

  
def scene_generator(base_dir):
    dt = datetime(2019, 1, 1)
    base_dir = base_dir.format(dt)
    # 1200Z to 2359
    c01_files = sorted(glob(os.path.join(base_dir, 'OR_ABI-L1b-RadF-M3C01_G16_s{:%Y%j}[12]*.nc').format(dt)))
    for c01_file in c01_files:
        ctime_idx = c01_file.find('e{:%Y}'.format(dt))
        all_files = glob(c01_file.replace('C01', 'C??')[:ctime_idx] + '*.nc')
        assert len(all_files) == 16
        
        scn = Scene(reader='abi_l1b', filenames=all_files)
        scn.load(ds_names)
        new_scn = scn.resample(scn.min_area(), resampler='native')
        yield new_scn

with ProgressBar():
    mscn = MultiScene(scene_generator(base_dir))
    #mscn.load(['C{:02d}'.format(x) for x in range(1, 17)])
    #new_mscn = mscn.resample(resampler='native')
    mscn.save_animation('{name}_{start_time:%Y%m%d_%H%M%S}.mp4', fps=10, batch_size=4)

# Join the individual videos together with ffmpeg
# - List files 
!for fn in C*.mp4; do echo "file '$fn'" >>channel_videos.txt; done
# - Create video
!ffmpeg -f concat -i channel_videos.txt -c copy channel_videos.mp4
# - Post processing 
!ffmpeg -i channel_videos.mp4 -vcodec libx264 -crf 38 abi_channel_videos.compress2.mp4


# GPM - Mesoscale matching !!!

