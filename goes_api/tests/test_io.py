#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 14:18:42 2022

@author: ghiggi
"""

# from trollsift import Parser
# fname = "OR_ABI-L2-ACHTF-M6_G16_s20200020100291_e20200020100349_c20200020101408.nc"
# fname = "OR_ABI-L2-ACHTM1-M6_G16_s20200020100291_e20200020100349_c20200020101408.nc"
# # {scene_abbr: ...} 1 letter and optionally a number (F,C, M1, M2)

# fpattern = '{system_environment:2s}_{mission_id:3s}-L2-{product_scene_abbr:s}-{scan_mode:2s}_{platform_shortname:3s}_s{start_time:%Y%j%H%M%S%f}_e{end_time:%Y%j%H%M%S%f}_c{creation_time:%Y%j%H%M%S%f}.nc'
# p = Parser(fpattern)
# info_dict = p.parse(fname)
# info_dict
# info_dict["product_scene_abbr"]


####--------------------------------------------------------------------------.
# Time selection

## Dates may be specified as datetime, pandas datetimes, or string dates
## that pandas can interpret.

## Specify start/end time with datetime object
# start = datetime(2021, 1, 1, 0, 30)
# end = datetime(2021, 1, 1, 1, 30)

## Specify start/end time as a panda-parsable string
start = "2021-01-01 00:30"
end = "2021-01-01 01:30"

# self.datetime= self.goes_sat_data.attrs['date_created']


# Test

PRODUCTS[sensor][product_level]
GLOB_FNAME_PATTERN[sensor][product_level]

dt = datetime.datetime(2020, 4, 12)

product_dir = get_product_dir(
    protocol, satellite, sensor, product_level, product, sector
)
year, doy, hour = dt_to_year_doy_hour(dt)
file_dir = os.path.join(product_dir, year, doy, hour)


fs = get_filesystem(protocol=protocol, fs_args=fs_args)
type(fs)

fs = fsspec.filesystem("s3", anon=True)

fs.glob("s3://noaa-goes17/ABI-L1b-RadF/2019/321/12/*.nc")

fpath = "s3://noaa-goes17/ABI-L1b-RadF/2019/321/12/OR_ABI-L1b-RadF-M6C16_G17_s20193211220341_e20193211229419_c20193211229473.nc"


open_directory_explorer(protocol="s3", satellite="goes-16")

get_key_from_filepaths(fpaths, sensor, product_level, key="start_time")

fpath_dict = find_files(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    group_by_key="start_time",
    verbose=True,
)

fpath_dict.keys()
fpath_dict

time = datetime.datetime(2021, 8, 11, 11, 25)
time = find_closest_start_time(
    time=datetime.datetime(2021, 8, 11, 11, 25),
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
)
time
fpath_dict = find_files(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=time,
    end_time=time,
    filter_parameters=filter_parameters,
    group_by_key="start_time",
    verbose=True,
)
fpath_dict

latest_time = find_latest_start_time(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    look_ahead_minutes=30,
)

print(latest_time - datetime.datetime.utcnow())  # 12 minute time lag !!!

start_time = time
N = 5
check_consistency = True
fpath_dict = find_previous_files(
    start_time=start_time,
    N=N,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    check_consistency=True,
)
fpath_dict.keys()
fpath_dict = find_next_files(
    start_time=start_time,
    N=N,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    check_consistency=True,
)


# group_by_key = "start_time"
# base_dir = "/home/ghiggi"
# check_satellite("16")
# check_satellite("G16")
# check_sensor("abi")
# check_sensor("ab")
# check_sector(sector, product="Rad") # NotImplementedError
# check_channel('Veggie')
# check_channel('1')
# check_channels(['Ozone','R','B'])
# check_scan_modes(scan_modes="M6")
# check_scan_modes(scan_modes="M3", sensor="GLM")

# _get_products_listing_dict(sensors=['ABI', 'GLM'], product_levels='L2')
# _get_products_sensor_dict()
# _get_products_product_level_dict()

# available_sensors()
# available_satellites()
# available_sectors()
# available_products()
# available_products(sensors=['ABI','GLM'])
# available_products(sensors=['GLM'], product_levels="L1B")

# protocol = check_protocol(protocol)
# base_dir = check_base_dir(base_dir)
# satellite = check_satellite(satellite)
# sensor = check_sensor(sensor)
# sector = check_sector(sector)
# product_level = check_product_level(product_level, product=None)
# product = check_product(product, sensor=sensor, product_level=product_level)
# start_time, end_time = check_start_end_time(start_time, end_time)

# filter_parameters = check_filter_parameters(filter_parameters, sensor, sector=sector)
# group_by_key = check_group_by_key(group_by_key, sensor, product_level)

# scan_modes = 'M3'
# channels = check_channels(channels, sensor=sensor)
# scan_modes = check_scan_modes(scan_modes=scan_modes, sensor=sensor)

from goes_api.io import _check_sector

_check_sector(sector="C", product="RRQPE")
