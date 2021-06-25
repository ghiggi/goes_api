#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 13:15:22 2020

@author: ghiggi
"""
# https://github.com/GoogleCloudPlatform/training-data-analyst/blob/master/blogs/lightning/ltgpred/goesutil/goesio.py 


from google.cloud import storage 
# gsutil ls gs://gcp-public-data-goes-16/ABI-L1b-RadF/2017/258/18/*C14*s201725818*
# import google.cloud.storage

def list_gcs(bucket, gcs_prefix, gcs_patterns):
  bucket = storage.Client().get_bucket(bucket)
  blobs = bucket.list_blobs(prefix=gcs_prefix, delimiter='/')
  result = []
  for b in blobs:
          match = True
          for pattern in gcs_patterns:
              if not pattern in b.path:
                match = False
          if match:
              result.append(b)
  return result

def blob_exists(projectname, credentials, bucket_name, filename):
   client = storage.Client(projectname, credentials=credentials)
   bucket = client.get_bucket(bucket_name)
   blob = bucket.blob(filename)
   stats = blob.exists()
   # bucket = client.bucket(bucket_name)
   # stats = storage.Blob(bucket=bucket, name=filename).exists(client)
   return stats

dest = 'gs://gcp-public-data-goes-16/ABI-L1b-RadF/2017/258/18/OR_ABI-L1b-RadF-M3C14_G16_s20172581800377_e20172581811144_c20172581811208.nc' 

def copy_fromgcs(bucket, objectId, destdir):
   import os.path
   import logging
   import google.cloud.storage as gcs
   bucket = gcs.Client().get_bucket(bucket)
   blob = bucket.blob(objectId)
   basename = os.path.basename(objectId)
   logging.info('Downloading {}'.format(basename))
   dest = os.path.join(destdir, basename)
   blob.download_to_filename(dest)
   return dest
 

with Dataset(ncfilename, 'r') as nc:
     rad = nc.variables['Rad'][:]
     ref = (rad * np.pi * 0.3) / 663.274497
     ref = np.minimum( np.maximum(ref, 0.0), 1.0 ) # scale to 0-1
     # crop to area of interest
     ref = crop_image(nc, ref, clat, clon)
     
     # do gamma correction to stretch the values
     ref = np.sqrt(ref)
     # plotting to jpg file
     fig = plt.figure()
     plt.imsave(outfile, ref, vmin=0.0, vmax=1.0, cmap='gist_ncar_r') # or 'Greys_r' without color
     plt.close('all')
     logging.info('Created {}'.format(outfile))
     return outfile
 
def crop_image(nc, data, clat, clon):
   import logging
   import numpy as np
   from pyproj import Proj
   import pyresample as pr

   # output grid centered on clat, clon in equal-lat-lon 
   lats = np.arange(clat-10,clat+10,0.01) # approx 1km resolution, 2000km extent
   lons = np.arange(clon-10,clon+10,0.01) # approx 1km resolution, 2000km extent
   lons, lats = np.meshgrid(lons, lats)
   new_grid = pr.geometry.GridDefinition(lons=lons, lats=lats)

   # Subsatellite_Longitude is where the GEO satellite is 
   lon_0 = nc.variables['nominal_satellite_subpoint_lon'][0]
   ht_0 = nc.variables['nominal_satellite_height'][0] * 1000 # meters
   x = nc.variables['x'][:] * ht_0 #/ 1000.0
   y = nc.variables['y'][:] * ht_0 #/ 1000.0
   nx = len(x)
   ny = len(y)
   max_x = x.max(); min_x = x.min(); max_y = y.max(); min_y = y.min()
   half_x = (max_x - min_x) / nx / 2.
   half_y = (max_y - min_y) / ny / 2.
   extents = (min_x - half_x, min_y - half_y, max_x + half_x, max_y + half_y)
   old_grid = pr.geometry.AreaDefinition('geos','goes_conus','geos', 
       {'proj':'geos', 'h':str(ht_0), 'lon_0':str(lon_0) ,'a':'6378169.0', 'b':'6356584.0'},
       nx, ny, extents)

   # now do remapping
   logging.info('Remapping from {}'.format(old_grid))
   return pr.kd_tree.resample_nearest(old_grid, data, new_grid, radius_of_influence=50000)

def plot_image(ncfilename, outfile, clat, clon):
    import matplotlib, logging
    matplotlib.use('Agg') # headless display
    import numpy as np
    from netCDF4 import Dataset
    import matplotlib.pyplot as plt
    
    with Dataset(ncfilename, 'r') as nc:
        rad = nc.variables['Rad'][:]
        # See http://www.goes-r.gov/products/ATBDs/baseline/Imagery_v2.0_no_color.pdf
        ref = (rad * np.pi * 0.3) / 663.274497
        ref = np.minimum( np.maximum(ref, 0.0), 1.0 )

        # crop to area of interest
        ref = crop_image(nc, ref, clat, clon)
        
        # do gamma correction to stretch the values
        ref = np.sqrt(ref)

        # plotting to jpg file
        fig = plt.figure()
        plt.imsave(outfile, ref, vmin=0.0, vmax=1.0, cmap='gist_ncar_r') # or 'Greys_r' without color
        plt.close('all')
        logging.info('Created {}'.format(outfile))
        return outfile
    return None    
 
## Retrieve Hurricane path with BigQuery 
 SELECT
 latitude,
 longitude,
 iso_time,
 dist2land
FROM
 `bigquery-public-data.noaa_hurricanes.hurricanes`
WHERE
 name LIKE '%MARIA%'
 AND season = '2017'
ORDER BY
 iso_time ASC
 
# Autoscaling data pipeline service 
# --> apache_beam