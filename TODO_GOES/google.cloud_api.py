#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 13:15:22 2020

@author: ghiggi
"""
# https://github.com/GoogleCloudPlatform/training-data-analyst/blob/master/blogs/lightning/ltgpred/goesutil/goesio.py 


from google.cloud import storage 
# gsutil ls gs://gcp-public-data-goes-16/ABI-L1b-RadF/2017/258/18/*C14*s201725818*
 

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
 

 