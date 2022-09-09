#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 16:33:44 2022

@author: ghiggi
"""
# https://github.com/pytroll/pytroll-examples/issues/35
# Post performance benchmark
# Correct cgentlemann, file and chunked and very small !!!

# Add
# - https://github.com/pytroll/satpy/issues/1062
# - https://github.com/pytroll/satpy/pull/1423

# todo: rerun benchmark with blockcache
# --->  need to optimize block_size
# --->  use simplecache
# - https://github.com/pytroll/satpy/pull/1321

####--------------------------------------------------------------------------.
#### IDEAS 
# - satpy accessor 

# - intake-satpy

# kerchunk with satpy
# - filename is a class wrapping kerchunk ... which has open() method implemented

####--------------------------------------------------------------------------.
