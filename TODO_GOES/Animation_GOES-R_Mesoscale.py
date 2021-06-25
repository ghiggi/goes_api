#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 17:08:27 2020

@author: ghiggi
"""
import os
from satpy import Scene
from glob import glob
from satpy.multiscene import MultiScene

### TODO: Download data automatically 
### TODO: Get file path 
BASE_DIR = '/data/data/abi/20180911_florence'
all_filenames = [glob(fn.replace('C01', 'C0[123]*')[:len(BASE_DIR) + 50] + '*.nc') for fn in sorted(glob(os.path.join(BASE_DIR, 'OR*-RadM1-*C01*.nc')))]
scenes = [Scene(reader='abi_l1b', filenames=filenames) for filenames in all_filenames]
print("Number of Scenes: ", len(scenes))

mscn = MultiScene(scenes)
mscn.load(['true_color'])
new_mscn = mscn.resample(resampler='native')
new_mscn.save_animation('/tmp/{name}_{start_time:%Y%m%d_%H%M%S}.mp4', fps=5)

# Load all ABI channels
channels = ['C{channel:02d}'.format(channel=chn) for chn in range(1, 17)]
 