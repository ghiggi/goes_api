#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 17:44:17 2023

@author: ghiggi
"""
import datetime 
from time import perf_counter
 

def print_elapsed_time(fn):
    def inner(*args, **kwargs):
        start_time = perf_counter()
        results = fn(*args, **kwargs)
        end_time = perf_counter()
        execution_time = end_time - start_time
        timedelta_str = str(datetime.timedelta(seconds=execution_time))
        print(f'Elapsed time: {timedelta_str} .', end="\n")
        return results
    return inner


 