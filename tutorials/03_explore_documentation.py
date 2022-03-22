#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 11:15:46 2022

@author: ghiggi
"""
import goes_api

goes_api.open_directory_explorer(satellite='goes16', protocol='s3')
goes_api.open_directory_explorer(satellite='goes16', protocol='gcp')
goes_api.open_directory_explorer(satellite='goes16', base_dir='/ltenas3/0_Data/GEO')

goes_api.open_ABI_channel_guide('C01')
goes_api.open_ABI_L2_product_guide('CTP')
goes_api.open_ABI_L2_product_guide('RRQPE')
