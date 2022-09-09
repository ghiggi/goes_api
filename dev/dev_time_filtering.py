#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 14:44:51 2022

@author: ghiggi
"""
filter_parameters = {'scan_modes': None,
 'channels': ['C01'],
 'scene_abbr': 'M2',
 'start_time': datetime.datetime(2019, 11, 17, 11, 30),
 'end_time': datetime.datetime(2019, 11, 17, 11, 30)} 

start_time = datetime.datetime(2019, 11, 17, 11, 30)
fpaths = ['s3://noaa-goes16/ABI-L1b-RadM/2019/321/11/OR_ABI-L1b-RadM2-M6C01_G16_s20193211130355_e20193211130412_c20193211130459.nc']
fpath = fpaths[0]
_filter_files(fpaths, sensor, product_level, **filter_parameters)

sensor = _check_sensor(sensor)
product_level = _check_product_level(product_level, product=None)
scan_modes = _check_scan_modes(scan_modes)
channels = _check_channels(channels, sensor=sensor)
scene_abbr = _check_scene_abbr(scene_abbr, sensor=sensor)
start_time, end_time = _check_start_end_time(start_time, start_time)
# _filter_file

# TODO
# -30 seconds scan