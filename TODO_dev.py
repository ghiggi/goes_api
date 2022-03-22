#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 11:04:53 2022

@author: ghiggi
"""
### Check consistency
# - Check for Mesoscale same location (on M1 and M2 separately) !
#   - FindFiles raise information when it changes !
# _check_unique_scan_mode: raise information when it changes
# _check_interval_regularity: raise info when missing between ... and ...

### kerchunk
# - Parallelize kerchunk reference file creation efficiently 
# - Benchmark dask.bag vs. dask_delayed vs. concurrent multithreading
# - MultiProcess seems to hang 

