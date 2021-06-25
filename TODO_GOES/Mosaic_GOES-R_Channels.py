#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 17:12:20 2020

@author: ghiggi
"""

import glob
from satpy.scene import Scene

## TODO Download data and get file path  

scn = Scene(
    filenames=glob.glob("/path/to/the/Goes-16/data/*RadF*"),
    reader='abi_l1b'
)

channels = ['C{channel:02d}'.format(channel=chn) for chn in range(1, 17)]
scn.load(channels)
scn.save_datasets(filename='{name}.png')


    
# Use the ImageMagick command montage to join all the images together
 
# GOES 8-15
# !montage C??.png -geometry 512x512+4+4 -background black montage_abi.jpg


# GOES 16-17
# montage ??_?.png -geometry 256x256+2-55 -background black goes-imager_nc_mosaic.jpg

for ch in channels:
    scene.load([ch])
    scene.save_dataset(ch, filename=ch+'.png')
    del scene[ch]