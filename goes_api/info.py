#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Ghiggi Gionata 

# goes_api is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# goes_api is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# goes_api. If not, see <http://www.gnu.org/licenses/>.

import os
import datetime
import numpy as np
from trollsift import Parser
from goes_api.checks import _check_group_by_key
from goes_api.alias import (
    BUCKET_PROTOCOLS,
    _satellites, 
    _sectors,
    _channels,
)


####--------------------------------------------------------------------------.
#### Dictionary retrievals


def get_available_online_product(protocol, satellite):
    """Get a dictionary of available products in a specific cloud bucket.

    The dictionary has structure {sensor: {product_level: [products]}}.

    Parameters
    ----------
    protocol : str
        String specifying the cloud bucket storage that you want to explore.
        Use `goes_api.available_protocols()` to retrieve available protocols.
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    """
    from goes_api.io import get_filesystem, get_bucket
        
    # Get filesystem and bucket
    fs = get_filesystem(protocol)
    bucket = get_bucket(protocol, satellite)
    # List contents of the satellite bucket.
    list_dir = fs.ls(bucket)
    list_dir = [path for path in list_dir if fs.isdir(path)]
    # Retrieve directories name
    list_dirname = [os.path.basename(f) for f in list_dir]
    # Remove sector letter for ABI folders
    list_products = [
        product[:-1] if product.startswith("ABI") else product
        for product in list_dirname
    ]
    list_products = np.unique(list_products).tolist()
    # Retrieve sensor, product_level and product list
    list_sensor_level_product = [product.split("-") for product in list_products]
    # Build a dictionary
    products_dict = {}
    for sensor, product_level, product in list_sensor_level_product:
        if products_dict.get(sensor) is None:
            products_dict[sensor] = {}
        if products_dict[sensor].get(product_level) is None:
            products_dict[sensor][product_level] = []
        products_dict[sensor][product_level].append(product)
    # Return dictionary
    return products_dict


def get_dict_info_products(sensors=None, product_levels=None):
    """Return a dictionary with sensors, product_level and product informations.

    The dictionary has structure {sensor: {product_level: [products]}}
    Specifying `sensors` and/or `product_levels` allows to retrieve only
    specific portions of the dictionary.
    """
    from goes_api.listing import PRODUCTS
    from goes_api.checks import _check_product_levels, _check_sensors

    if sensors is None and product_levels is None:
        return PRODUCTS
    if sensors is None:
        sensors = available_sensors()
    if product_levels is None:
        product_levels = available_product_levels()
    # Subset by sensors
    sensors = _check_sensors(sensors)
    intermediate_listing = {sensor: PRODUCTS[sensor] for sensor in sensors}
    # Subset by product_levels
    product_levels = _check_product_levels(product_levels)
    listing_dict = {}
    for sensor, product_level_dict in intermediate_listing.items():
        for product_level, products_dict in product_level_dict.items():
            if product_level in product_levels:
                if listing_dict.get(sensor) is None:
                    listing_dict[sensor] = {}
                listing_dict[sensor][product_level] = products_dict
    # Return filtered listing_dict
    return listing_dict


def get_dict_product_sensor(sensors=None, product_levels=None):
    """Return a dictionary with available product and corresponding sensors.

    The dictionary has structure {product: sensor}.
    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the dictionary.
    """
    # Get product listing dictionary
    products_listing_dict = get_dict_info_products(
        sensors=sensors, product_levels=product_levels
    )
    # Retrieve dictionary
    products_sensor_dict = {}
    for sensor, product_level_dict in products_listing_dict.items():
        for product_level, products_dict in product_level_dict.items():
            for product in products_dict.keys():
                products_sensor_dict[product] = sensor
    return products_sensor_dict


def get_dict_sensor_products(sensors=None, product_levels=None):
    """Return a dictionary with available sensors and corresponding products.

    The dictionary has structure {sensor: [products]}.
    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the dictionary.
    """
    products_sensor_dict = get_dict_product_sensor(
        sensors=sensors, product_levels=product_levels
    )
    sensor_product_dict = {}
    for k in set(products_sensor_dict.values()):
        sensor_product_dict[k] = []
    for product, sensor in products_sensor_dict.items():
        sensor_product_dict[sensor].append(product)
    return sensor_product_dict


def get_dict_product_product_level(sensors=None, product_levels=None):
    """Return a dictionary with available products and corresponding product_level.

    The dictionary has structure {product: product_level}.
    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the dictionary.
    """
    # Get product listing dictionary
    products_listing_dict = get_dict_info_products(
        sensors=sensors, product_levels=product_levels
    )
    # Retrieve dictionary
    products_product_level_dict = {}
    for sensor, product_level_dict in products_listing_dict.items():
        for product_level, products_dict in product_level_dict.items():
            for product in products_dict.keys():
                products_product_level_dict[product] = product_level
    return products_product_level_dict


def get_dict_product_level_products(sensors=None, product_levels=None):
    """Return a dictionary with available product_levels and corresponding products.

    The dictionary has structure {product_level: [products]}.
    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the dictionary.
    """
    products_product_level_dict = get_dict_product_product_level(
        sensors=sensors, product_levels=product_levels
    )
    product_level_product_dict = {}
    for k in set(products_product_level_dict.values()):
        product_level_product_dict[k] = []
    for product, sensor in products_product_level_dict.items():
        product_level_product_dict[sensor].append(product)
    return product_level_product_dict


####--------------------------------------------------------------------------.
#### Availability


def available_protocols():
    """Return a list of available cloud bucket protocols."""
    return BUCKET_PROTOCOLS


def available_sensors():
    """Return a list of available sensors."""
    from goes_api.listing import PRODUCTS

    return list(PRODUCTS.keys())


def available_satellites():
    """Return a list of available satellites."""
    return list(_satellites.keys())


def available_sectors(product=None):
    """Return a list of available sectors.

    If `product` is specified, it returns the sectors available for such specific
    product.
    """
    from goes_api.listing import ABI_L2_SECTOR_EXCEPTIONS
    from goes_api.checks import _check_product

    sectors_keys = list(_sectors.keys())
    if product is None:
        return sectors_keys
    else:
        product = _check_product(product)
        specific_sectors = ABI_L2_SECTOR_EXCEPTIONS.get(product)
        if specific_sectors is None:
            return sectors_keys
        else:
            return specific_sectors


def available_product_levels(sensors=None):
    """Return a list of available product levels.

    If `sensors` is specified, it returns the product levels available for
    the specified set of sensors.
    """

    from goes_api.listing import PRODUCTS

    if sensors is None:
        return ["L1b", "L2"]
    else:
        if isinstance(sensors, str):
            sensors = [sensors]
        product_levels = np.concatenate(
            [list(PRODUCTS[sensor].keys()) for sensor in sensors]
        )
        product_levels = np.unique(product_levels).tolist()
        return product_levels


def available_scan_modes():
    """Return available scan modes for ABI.

    Scan modes:
    - M3: Default scan strategy before 2 April 2019.
          --> Full Disk every 15 minutes
          --> CONUS/PACUS every 5 minutes
          --> M1 and M2 every 1 minute
    - M6: Default scan strategy since 2 April 2019.
          --> Full Disk every 10 minutes
          --> CONUS/PACUS every 5 minutes
          --> M1 and M2 every 5 minutes
    - M4: Only Full Disk every 5 minutes
          --> Example: Dec 4 2018, ...
    """
    scan_mode = ["M3", "M4", "M6"]
    return scan_mode


def available_channels():
    """Return a list of available ABI channels."""
    channels = list(_channels.keys())
    return channels


def available_products(sensors=None, product_levels=None):
    """Return a list of available products.

    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the list.
    """
    # Get product listing dictionary
    products_dict = get_dict_product_sensor(
        sensors=sensors, product_levels=product_levels
    )
    products = list(products_dict.keys())
    return products


def available_group_keys():
    """Return a list of available group_keys."""
    group_keys = [
        "system_environment",
        "sensor",  # ABI
        "product_level",
        "product",       # ...
        "scene_abbr",  # ["F", "C", "M1", "M2"]
        "scan_mode",  # ["M3", "M4", "M6"]
        "channel",  # C**
        "platform_shortname",  # G16, G17
        "start_time",
        "end_time",
    ]
    return group_keys


def available_connection_types():
    """Return a list of available connect_type to connect to cloud buckets."""
    return ["bucket", "https", "nc_bytes"]


####---------------------------------------------------------------------------.
#### Information extraction from filepaths


def infer_satellite_from_path(path): 
    """Infer the satellite from the file path."""
    goes16_patterns = ['goes16', 'goes-16', 'G16']
    goes17_patterns = ['goes17', 'goes-17', 'G17'] 
    if np.any([pattern in path for pattern in goes16_patterns]):
        return 'goes-16'
    if np.any([pattern in path for pattern in goes17_patterns]):
        return 'goes-17'
    else:
        raise ValueError("Unexpected GOES file path.")
        

def _infer_product_level(fpath):
    """Infer product_level from filepath."""
    fname = os.path.basename(fpath)
    if '-L1b-' in fname: 
        return 'L1b'
    elif '-L2-' in fname: 
        return 'L2'
    else: 
        raise ValueError(f"`product_level` could not be inferred from {fname}.")


def _infer_sensor(fpath):
    """Infer sensor from filepath."""
    fname = os.path.basename(fpath)
    if '_ABI-' in fname: 
        return 'ABI'
    elif '_EXIS-' in fname: 
        return 'EXIS'
    elif '_GLM-' in fname: 
        return 'GLM'
    elif '_MAG-' in fname: 
        return 'MAG'
    elif '_SEIS-' in fname: 
        return 'SEIS'
    elif '_SUVI-' in fname: 
        return 'SUVI'
    else: 
        raise ValueError(f"`sensor` could not be inferred from {fname}.")


def _infer_satellite(fpath):
    """Infer satellite from filepath."""
    fname = os.path.basename(fpath)
    if '_G16_' in fname: 
        return 'GOES-16'
    elif '_G17_-' in fname: 
        return 'GOES-17'
    else: 
        raise ValueError(f"`satellite` could not be inferred from {fname}.")


def _separate_product_scene_abbr(product_scene_abbr):
    """Return (product, scene_abbr) from <product><scene_abbr> string."""
    last_letter = product_scene_abbr[-1]
    # Mesoscale domain
    if last_letter in ["1", "2"]:
        return product_scene_abbr[:-2], product_scene_abbr[-2:]
    # CONUS and Full Disc
    elif last_letter in ["F", "C"]:
        return product_scene_abbr[:-1], product_scene_abbr[-1]
    else:
        raise NotImplementedError("Adapat the file patterns.")

def _round_datetime_to_nearest_minute(time):
    """Round datetime time to nearest minute."""
    # Add half a minute to the datetime object
    rounded = time + datetime.timedelta(seconds=30)
    # Truncate the seconds and microseconds
    rounded = rounded.replace(second=0, microsecond=0)
    return rounded


def _get_info_from_filename(fname):
    """Retrieve file information dictionary from filename."""
    from goes_api.listing import GLOB_FNAME_PATTERN
    # Infer sensor and product_level
    sensor = _infer_sensor(fname)
    product_level = _infer_product_level(fname)
        
    # Retrieve file pattern
    fpattern = GLOB_FNAME_PATTERN[sensor][product_level]
    
    # Retrieve information from filename 
    p = Parser(fpattern)
    info_dict = p.parse(fname)
    
    # For sensor other than ABI, add scan_mode = ""
    if "scan_mode" not in info_dict:
        info_dict["scan_mode"] = ""
    
    # Assert sensor and product_level are correct
    assert sensor == info_dict['sensor']
    assert product_level == info_dict['product_level']
    
    # Round start_time and end_time to minute resolution
    info_dict["start_time"] =  _round_datetime_to_nearest_minute(info_dict["start_time"]) 
    info_dict["end_time"] =  _round_datetime_to_nearest_minute(info_dict["end_time"]) 

    # Special treatment for ABI L2 products
    if info_dict.get("product_scene_abbr") is not None:
        # Identify scene_abbr
        product, scene_abbr = _separate_product_scene_abbr(
            info_dict.get("product_scene_abbr")
        )
        info_dict["product"] = product
        info_dict["scene_abbr"] = scene_abbr
        del info_dict["product_scene_abbr"]
        # Special treatment for CMIP to extract channels
        if product == "CMIP":
            scan_mode_channels = info_dict["scan_mode"]
            scan_mode = scan_mode_channels[0:3]
            channels = scan_mode_channels[3:]
            info_dict["scan_mode"] = scan_mode
            info_dict["channels"] = channels
            
    # Special treatment for ABI products to retrieve sector 
    if sensor == 'ABI':
        if 'M' in info_dict["scene_abbr"]:
            sector = 'M'
        else: 
            sector = info_dict["scene_abbr"] 
        info_dict["sector"] =  sector   
    
    # Derive satellite name  
    platform_shortname = info_dict["platform_shortname"]
    if 'G16' == platform_shortname:
        satellite = 'GOES-16'
    elif 'G17' == platform_shortname:
        satellite = 'GOES-17'
    elif 'G18' == platform_shortname:
         satellite = 'GOES-18'
    else:
        raise ValueError(f"Processing of satellite {platform_shortname} not yet implemented.")
    info_dict["satellite"] =  satellite  
        
    # Return info dictionary
    return info_dict


def _get_info_from_filepath(fpath):
    """Retrieve file information dictionary from filepath."""
    if not isinstance(fpath, str):
        raise TypeError("'fpath' must be a string.")
    fname = os.path.basename(fpath)
    return _get_info_from_filename(fname)


def _get_key_from_filepaths(fpaths, key):
    """Extract specific key information from a list of filepaths."""
    if isinstance(fpaths, str):
        fpaths = [fpaths]
    return [
        _get_info_from_filepath(fpath)[key] for fpath in fpaths
    ]


def get_key_from_filepaths(fpaths, key):
    """Extract specific key information from a list of filepaths."""
    if isinstance(fpaths, dict):
        fpaths = {k: _get_key_from_filepaths(v, key=key) for k, v in fpaths.items()}
    else:
        fpaths = _get_key_from_filepaths(fpaths, key=key)
    return fpaths 


####---------------------------------------------------------------------------.
#### Group filepaths 


def _group_fpaths_by_key(fpaths, key="start_time"):
    """Utils function to group filepaths by key contained into filename."""
    # - Retrieve key sorting index 
    list_key_values = [
        _get_info_from_filepath(fpath)[key] for fpath in fpaths
    ]
    idx_key_sorting = np.array(list_key_values).argsort()
    # - Sort fpaths and key_values by key values
    fpaths = np.array(fpaths)[idx_key_sorting]
    list_key_values = np.array(list_key_values)[idx_key_sorting]
    # - Retrieve first occurence of new key value
    unique_key_values, cut_idx = np.unique(list_key_values, return_index=True)
    # - Split by key value
    fpaths_grouped = np.split(fpaths, cut_idx)[1:]
    # - Convert array of fpaths into list of fpaths 
    fpaths_grouped = [arr.tolist() for arr in fpaths_grouped]
    # - Create (key: files) dictionary
    fpaths_dict = dict(zip(unique_key_values, fpaths_grouped))
    return fpaths_dict


def group_files(fpaths, key="start_time"):
    """
    Group filepaths by key contained into filenames.

    Parameters
    ----------
    fpaths : list
        List of filepaths.
    key : str
        Key by which to group the list of filepaths.
        The default key is "start_time".
        See `goes_api.available_group_keys()` for available grouping keys.

    Returns
    -------
    fpaths_dict : dict
        Dictionary with structure {<key>: list_fpaths_with_<key>}.

    """
    if isinstance(fpaths, dict): 
        raise TypeError("It's not possible to group a dictionary ! Pass a list of filepaths instead.")
    key = _check_group_by_key(key)
    fpaths_dict = _group_fpaths_by_key(fpaths=fpaths, key=key)
    return fpaths_dict

