#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 11:04:16 2022

@author: ghiggi
"""
#-----------------------------------------------------------------------------.
# Raw data storage requirements for 1 satellite:
# 4 GB per hour
# 100-120 GB per day 
# 3 TB per month

#-----------------------------------------------------------------------------.
# https://github.com/blaylockbk/pyBKB_v3/blob/master/rclone_howto.md 
# curl https://rclone.org/install.sh | sudo bash
# sudo apt update; sudo apt install -y libgnutls30

#-----------------------------------------------------------------------------.
# DOY 
# first july: 182 
# 15 august: 227

import os 
for doy in range(187, 227):
    src_fpath = "publicAWS:noaa-goes16/ABI-L1b-RadF/2020/" + str(doy)
    dst_fpath = "/ltenas3/0_Data/GEO/GOES16/ABI-L1b-RadF/2020/" + str(doy)
    cmd_fpath = "rclone copy " + src_fpath + " " + dst_fpath
    print(doy)
    os.system(cmd_fpath)


## Download GOES data 
# for doy in {001...365} do aws s3 sync s3://noaa-goes16/blahblah ./ & done 

#-----------------------------------------------------------------------------.
#### Scripted/bulk downloading
### TO download
# rclone, s3fs
# GOES-2-Go: https://github.com/blaylockbk/goes2go
# goespy: https://github.com/spestana/goes-py
# goes-mirror: https://github.com/meteoswiss-mdr/goesmirror
# GOES: https://github.com/joaohenry23/GOES/tree/master/GOES
# https://github.com/blaylockbk/pyBKB_v3/blob/master/rclone_howto.md
# Satpy and GOES-2-Go packages automate the generation of common GOES-R composites

## Interactive download 
# https://home.chpc.utah.edu/~u0553130/Brian_Blaylock/cgi-bin/goes16_download.cgi
# https://home.chpc.utah.edu/~u0553130/Brian_Blaylock/cgi-bin/generic_AWS_download.cgi?DATASET=noaa-goes16