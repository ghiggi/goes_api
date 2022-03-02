#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 26 17:30:43 2022

@author: ghiggi
"""
import os
import fsspec
import datetime
import numpy as np
import pandas
from trollsift import Parser

####--------------------------------------------------------------------------.
#### Alias
_satellites = {
    "goes-16": ["16", "G16", "GOES-16", "GOES16", "EAST", "GOES-EAST"],
    "goes-17": ["17", "G17", "GOES-17", "GOES17", "WEST", "GOES-WEST"],
}

_sectors = {
    "C": ["CONUS", "PACUS", "C", "P"],
    "F": ["FULL", "FULLDISK", "FULL DISK", "F"],
    "M": ["MESOSCALE", "M1", "M2", "M"],
}

_channels = {
    "C01": ["C01", "1", "01", "0.47", "BLUE", "B"],
    "C02": ["C02", "2", "02", "0.64", "RED", "R"],
    "C03": ["C03", "3", "03", "0.86", "VEGGIE"],  # G
    "C04": ["C04", "4", "04", "1.37", "CIRRUS"],
    "C05": ["C05", "5", "05", "1.6", "SNOW/ICE"],
    "C06": ["C06", "6", "06", "2.2", "CLOUD PARTICLE SIZE", "CPS"],
    "C07": ["C07", "7", "07", "3.9", "IR SHORTWAVE WINDOW", "IR SHORTWAVE"],
    "C08": ["C08", "8", "08", "6.2", "UPPER-LEVEL TROPOSPHERIC WATER VAPOUR",
            "UPPER-LEVEL WATER VAPOUR"],
    "C09": ["C09", "9", "09", "6.9", "MID-LEVEL TROPOSPHERIC WATER VAPOUR",
            "MID-LEVEL WATER VAPOUR"],
    "C10": ["C10", "10", "10", "7.3", "LOWER-LEVEL TROPOSPHERIC WATER VAPOUR",
            "LOWER-LEVEL WATER VAPOUR"],
    "C11": ["C11", "11", "11", "8.4", "CLOUD-TOP PHASE", "CTP"],
    "C12": ["C12", "12", "12", "9.6", "OZONE"],
    "C13": ["C13", "13", "10.3", "CLEAN IR LONGWAVE WINDOW", "CLEAN IR"],
    "C14": ["C14", "14", "11.2", "IR LONGWAVE WINDOW", "IR LONGWAVE"],
    "C15": ["C15", "15", "12.3", "DIRTY LONGWAVE WINDOW", "DIRTY IR"],
    "C16": ["C16", "16", "13.3", "CO2 IR LONGWAVE", "CO2", "CO2 IR"],
}

PROTOCOLS = ["gcs", "s3", "local", "file"]

####--------------------------------------------------------------------------.
#### Availability
def available_sensors():
    from goes_api.listing import PRODUCTS

    return list(PRODUCTS.keys())


def available_satellites():
    return list(_satellites.keys())


def available_sectors(product=None):
    if product is None:
        return list(_sectors.keys())
    # TODO
    raise NotImplementedError()


def available_product_levels(sensors=None):
    from goes_api.listing import PRODUCTS

    if sensors is None:
        return ["L1b", "L2"]
    else:
        if isinstance(sensors, str):
            sensors = [sensors]
        return np.unique(
            np.concatenate([list(PRODUCTS[sensor].keys()) for sensor in sensors])
        ).tolist()


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
    channels = list(_channels.keys())
    return channels


def available_products(sensors=None, product_levels=None):
    return _get_products(sensors=sensors, product_levels=product_levels)


####--------------------------------------------------------------------------.
####
def check_protocol(protocol):
    if protocol is not None:
        if not isinstance(protocol, str):
            raise TypeError("`protocol` must be a string.")
        if protocol not in PROTOCOLS:
            raise ValueError(f"Valid `protocol` are {PROTOCOLS}.")
        if protocol == "local":
            protocol = "file"  # for fsspec LocalFS compatibility
    return protocol


def check_base_dir(base_dir):
    if base_dir is not None:
        if not isinstance(base_dir, str):
            raise TypeError("`base_dir` must be a string.")
        if not os.path.exists(base_dir):
            raise OSError(f"`base_dir` {base_dir} does not exist.")
        if not os.path.isdir(base_dir):
            raise OSError(f"`base_dir` {base_dir} is not a directory.")
    return base_dir


def check_satellite(satellite):
    if not isinstance(satellite, str):
        raise TypeError("`satellite` must be a string.")
    # Retrieve satellite key accounting for possible aliases
    satellite_key = None
    for key, possible_values in _satellites.items():
        if satellite.upper() in possible_values:
            satellite_key = key
            break
    if satellite_key is None:
        valid_satellite_key = list(_satellites.keys())
        raise ValueError(f"Available satellite: {valid_satellite_key}")
    return satellite_key


def check_sector(sector, product=None):
    if not isinstance(sector, str):
        raise TypeError("`sector` must be a string.")
    # Retrieve sector key accounting for possible aliases
    sector_key = None
    for key, possible_values in _sectors.items():
        if sector.upper() in possible_values:
            sector_key = key
            break
    if sector_key is None:
        valid_sector_keys = list(_sectors.keys())
        raise ValueError(f"Available satellite: {valid_sector_keys}")
    # Check the sector is valid for a given product (if specified)
    valid_sectors = available_sectors(product=product)
    if product is not None:
        if sector_key not in valid_sectors:
            raise ValueError(
                f"Valid sectors for product {product} are {valid_sectors}."
            )
    return sector_key


def check_sensor(sensor):
    if not isinstance(sensor, str):
        raise TypeError("`sensor` must be a string.")
    valid_sensors = available_sensors()
    sensor = sensor.upper()
    if sensor not in valid_sensors:
        raise ValueError(f"Available sensors: {valid_sensors}")
    return sensor


def check_sensors(sensors):
    if isinstance(sensors, str):
        sensors = [sensors]
    sensors = [check_sensor(sensor) for sensor in sensors]
    return sensors


def check_product_level(product_level, product=None):
    if not isinstance(product_level, str):
        raise TypeError("`product_level` must be a string.")
    product_level = product_level.capitalize()
    if product_level not in ["L1b", "L2"]:
        raise ValueError("Available product levels are ['L1b', 'L2'].")
    if product is not None:
        if product not in get_product_level_products_dict()[product_level]:
            raise ValueError(
                f"`product_level` '{product_level}' does not include product '{product}'."
            )
    return product_level


def check_product_levels(product_levels):
    if isinstance(product_levels, str):
        product_levels = [product_levels]
    product_levels = [
        check_product_level(product_level) for product_level in product_levels
    ]
    return product_levels


def check_product(product, sensor=None, product_level=None):
    if not isinstance(product, str):
        raise TypeError("`product` must be a string.")
    valid_products = available_products(sensors=sensor, product_levels=product_level)
    # Retrieve product by accounting for possible aliases (upper/lower case)
    product_key = None
    for possible_values in valid_products:
        if possible_values.upper() == product.upper():
            product_key = possible_values
            break
    if product_key is None:
        if sensor is None and product_level is None:
            raise ValueError(f"Available products: {valid_products}")
        else:
            sensor = "" if sensor is None else sensor
            product_level = "" if product_level is None else product_level
            raise ValueError(
                f"Available {product_level} products for {sensor}: {valid_products}"
            )
    return product_key


def check_time(time):
    if not isinstance(time, (datetime.datetime, str)):
        raise TypeError(
            "Specify time with datetime.datetime objects or a "
            "string of format 'YYYY-MM-DD hh:mm:ss'."
        )
    if isinstance(time, str):
        try:
            time = datetime.datetime.fromisoformat(time)
        except ValueError:
            raise ValueError("The time string must have format 'YYYY-MM-DD hh:mm:ss'")
    return time


def check_start_end_time(start_time, end_time):
    # Format input
    start_time = check_time(start_time)
    end_time = check_time(end_time)
    # Set resolution to minutes (TODO: CONSIDER POSSIBLE MESOSCALE AT 30 SECS)
    start_time = start_time.replace(microsecond=0, second=0)
    end_time = end_time.replace(microsecond=0, second=0)
    # Check start_time and end_time are chronological
    if start_time > end_time:
        raise ValueError("Provide start_time occuring before of end_time")
    # Check start_time and end_time are in the past
    if start_time > datetime.datetime.utcnow():
        raise ValueError("Provide a start_time occuring in the past.")
    if end_time > datetime.datetime.utcnow():
        raise ValueError("Provide a end_time occuring in the past.")
    return (start_time, end_time)


def check_channel(channel):
    if not isinstance(channel, str):
        raise TypeError("`channel` must be a string.")
    # Check channel follow standard name
    channel = channel.upper()
    if channel in list(_channels.keys()):
        return channel
    # Retrieve channel key accounting for possible aliases
    else:
        channel_key = None
        for key, possible_values in _channels.items():
            if channel.upper() in possible_values:
                channel_key = key
                break
        if channel_key is None:
            valid_channels_key = list(_channels.keys())
            raise ValueError(f"Available channels: {valid_channels_key}")
        return channel_key


def check_channels(channels=None, sensor=None):
    if channels is None:
        return channels
    if sensor is not None:
        if sensor != "ABI":
            raise ValueError("`sensor` must be 'ABI' if the channels are specified!")
    if isinstance(channels, str):
        channels = [channels]
    channels = [check_channel(channel) for channel in channels]
    return channels


def check_scan_mode(scan_mode):
    if not isinstance(scan_mode, str):
        raise TypeError("`scan_mode` must be a string.")
    # Check channel follow standard name
    scan_mode = scan_mode.upper()
    valid_scan_modes = available_scan_modes()
    if scan_mode in valid_scan_modes:
        return scan_mode
    else:
        raise ValueError(f"Available `scan_mode`: {valid_scan_modes}")


def check_scan_modes(scan_modes=None, sensor=None):
    if scan_modes is None:
        return scan_modes
    if sensor is not None:
        if sensor != "ABI":
            raise ValueError("`sensor` must be 'ABI' if the scan_mode is specified!")
    if isinstance(scan_modes, str):
        scan_modes = [scan_modes]
    scan_modes = [check_scan_mode(scan_mode) for scan_mode in scan_modes]
    return scan_modes


def check_scene_abbr(scene_abbr, sensor=None, sector=None):
    if scene_abbr is None:
        return scene_abbr
    if sensor is not None:
        if sensor != "ABI":
            raise ValueError("`sensor` must be 'ABI' if the scene_abbr is specified!")
    if sector is not None:
        if sector != "M":
            raise ValueError("`scene_abbr` must be specified only if sector=='M' !")
    if not isinstance(scene_abbr, (str, list)):
        raise TypeError("Specify `scene_abbr` as string or list.")
    if isinstance(scene_abbr, list):
        if len(scene_abbr) == 1:
            scene_abbr = scene_abbr[0]
        else:
            return None  # set to None assuming ['M1' and 'M2']
    if scene_abbr not in ["M1", "M2"]:
        raise ValueError("Valid `scene_abbr` values are 'M1' or 'M2'.")
    return scene_abbr


def check_filter_parameters(filter_parameters, sensor, sector):
    """Check filter parameters validity.

    It ensures that scan_modes, channels, scene_abbr are valid list (or None).
    """
    if not isinstance(filter_parameters, dict):
        raise TypeError("filter_parameters must be a dictionary.")
    scan_modes = filter_parameters.get("scan_modes")
    channels = filter_parameters.get("channels")
    scene_abbr = filter_parameters.get("scene_abbr")
    if scan_modes:
        filter_parameters["scan_modes"] = check_scan_modes(scan_modes)
    if channels:
        filter_parameters["channels"] = check_channels(channels, sensor=sensor)
    if scene_abbr:
        filter_parameters["scene_abbr"] = check_scene_abbr(
            scene_abbr, sensor=sensor, sector=sector
        )
    return filter_parameters


def check_group_by_key(group_by_key, sensor=None, product_level=None):
    if not isinstance(group_by_key, (str, type(None))):
        raise TypeError("`group_by_key`must be a string or None.")
    # TODO: Add checks !!!
    return group_by_key


def check_connection_type(connection_type, protocol):
    if not isinstance(connection_type, (str, type(None))):
        raise TypeError("`connection_type` must be a string (or None).")
    if protocol is None:
        connection_type = None
    if protocol in ["file", "local"]:
        connection_type = None  # set default
    if protocol in ["gcs", "s3"]:
        # Set default connection type
        if connection_type is None:
            connection_type = "bucket"  # set default
        valid_connection_type = ["bucket", "https", "nc_bytes"]
        if connection_type not in valid_connection_type:
            raise ValueError(f"Valid `connection_type` are {valid_connection_type}.")
    return connection_type


def check_unique_scan_mode(fpath_dict, sensor, product_level):
    # TODO: raise information when it changes
    list_datetime = list(fpath_dict.keys())
    fpaths_examplars = [fpath_dict[tt][0] for tt in list_datetime]
    list_scan_modes = get_key_from_filepaths(
        fpaths_examplars, sensor, product_level, key="scan_mode"
    )
    list_scan_modes = np.unique(list_scan_modes).tolist()
    if len(list_scan_modes) != 1:
        raise ValueError(
            f"There is a mixture of the following scan_mode: {list_scan_modes}."
        )


def check_interval_regularity(list_datetime):
    # TODO: raise info when missing between ... and ...
    if len(list_datetime) < 2:
        raise ValueError("Provide a list with at least 2 datetime.")
    list_datetime = sorted(list_datetime)
    list_timedelta = np.diff(list_datetime)
    list_unique_timedelta = np.unique(list_timedelta)
    if len(list_unique_timedelta) != 1:
        raise ValueError("The time interval is not regular!")


####--------------------------------------------------------------------------.
#### Dictionary retrievals
def _get_products_listing_dict(sensors=None, product_levels=None):
    from goes_api.listing import PRODUCTS

    if sensors is None and product_levels is None:
        return PRODUCTS
    if sensors is None:
        sensors = available_sensors()
    if product_levels is None:
        product_levels = available_product_levels()
    # Subset by sensors
    sensors = check_sensors(sensors)
    intermediate_listing = {sensor: PRODUCTS[sensor] for sensor in sensors}
    # Subset by product_levels
    product_levels = check_product_levels(product_levels)
    listing_dict = {}
    for sensor, product_level_dict in intermediate_listing.items():
        for product_level, products_dict in product_level_dict.items():
            if product_level in product_levels:
                if listing_dict.get(sensor) is None:
                    listing_dict[sensor] = {}
                listing_dict[sensor][product_level] = products_dict
    # Return filtered listing_dict
    return listing_dict


def _get_products_sensor_dict(sensors=None, product_levels=None):
    # Get product listing dictionary
    products_listing_dict = _get_products_listing_dict(
        sensors=sensors, product_levels=product_levels
    )
    # Retrieve dictionary
    products_sensor_dict = {}
    for sensor, product_level_dict in products_listing_dict.items():
        for product_level, products_dict in product_level_dict.items():
            for product in products_dict.keys():
                products_sensor_dict[product] = sensor
    return products_sensor_dict


def get_sensor_products_dict(sensors=None, product_level=None):
    products_sensor_dict = _get_products_sensor_dict(
        sensors=sensors, product_level=product_level
    )
    sensor_product_dict = {}
    for k in set(products_sensor_dict.values()):
        sensor_product_dict[k] = []
    for product, sensor in products_sensor_dict.items():
        sensor_product_dict[sensor].append(product)
    return sensor_product_dict


def _get_products_product_level_dict(sensors=None, product_levels=None):
    # Get product listing dictionary
    products_listing_dict = _get_products_listing_dict(
        sensors=sensors, product_levels=product_levels
    )
    # Retrieve dictionary
    products_product_level_dict = {}
    for sensor, product_level_dict in products_listing_dict.items():
        for product_level, products_dict in product_level_dict.items():
            for product in products_dict.keys():
                products_product_level_dict[product] = product_level
    return products_product_level_dict


def get_product_level_products_dict():
    products_product_level_dict = _get_products_product_level_dict()
    product_level_product_dict = {}
    for k in set(products_product_level_dict.values()):
        product_level_product_dict[k] = []
    for product, sensor in products_product_level_dict.items():
        product_level_product_dict[sensor].append(product)
    return product_level_product_dict


def _get_products(sensors=None, product_levels=None):
    # Get product listing dictionary
    products_dict = _get_products_sensor_dict(
        sensors=sensors, product_levels=product_levels
    )
    products = list(products_dict.keys())
    return products


####--------------------------------------------------------------------------.
#### Filesystems, buckets and directory structures
def get_filesystem(protocol, fs_args={}):
    if not isinstance(fs_args, dict):
        raise TypeError("fs_args must be a dictionary.")
    if protocol == "s3":
        # Set defaults
        # - Use the anonymous credentials to access public data
        _ = fs_args.setdefault("anon", True)  # TODO: or if is empty
        fs = fsspec.filesystem("s3", **fs_args)
        return fs
    elif protocol == "gcs":
        # Set defaults
        # - Use the anonymous credentials to access public data
        _ = fs_args.setdefault("token", "anon")  # TODO: or if is empty
        fs = fsspec.filesystem("gcs", **fs_args)
        return fs
    elif protocol in ["local", "file"]:
        fs = fsspec.filesystem("file")
        return fs
    else:
        raise NotImplementedError(
            "Current available protocols are 'gcs', 's3', 'local'."
        )


def get_bucket(protocol, satellite):
    # Dictionary of bucket and urls
    bucket_dict = {
        "gcs": "gs://gcp-public-data-{}".format(satellite),
        "s3": "s3://noaa-{}".format(satellite.replace("-", "")),
        "oracle": os.path.join(
            "https://objectstorage.us-ashburn-1.oraclecloud.com/n/idcxvbiyd8fn/b/goes/o/",
            satellite,
        ),
        "adl": os.path.join(
            "https://goeseuwest.blob.core.windows.net/noaa-", satellite
        ),  # # EU west
        # "adl": os.path.join("https://goes.blob.core.windows.net/noaa-", satellite),  # East US subset
    }
    return bucket_dict[protocol]


fs = fsspec.filesystem("https")


def _switch_to_https_fpaths(fpaths, protocol, satellite):
    # https://storage.googleapis.com , https://storage.cloud.google.com
    https_base_url_dict = {
        "gcs": "https://storage.cloud.google.com/gcp-public-data-{}".format(satellite),
        "s3": "https://noaa-{}.s3.amazonaws.com".format(satellite.replace("-", "")),
    }
    base_url = https_base_url_dict[protocol]
    fpaths = [os.path.join(base_url, fpath.split("/", 3)[3]) for fpath in fpaths]
    return fpaths


def get_bucket_prefix(protocol):
    if protocol == "gcs":
        prefix = "gs://"
    elif protocol == "s3":
        prefix = "s3://"
    elif protocol == "file":
        prefix = ""
    else:
        raise NotImplementedError(
            "Current available protocols are 'gcs', 's3', 'local'."
        )
    return prefix


def get_product_name(sensor, product_level, product, sector):
    product_name = f"{sensor}-{product_level}-{product}{sector}"
    return product_name


def get_product_dir(
    satellite, sensor, product_level, product, sector, protocol=None, base_dir=None
):
    if base_dir is None:
        bucket = get_bucket(protocol, satellite)
    else:
        bucket = os.path.join(base_dir, satellite.upper())
        if not os.path.exists(bucket):
            raise OSError(f"The directory {bucket} does not exist.")
    product_name = get_product_name(sensor, product_level, product, sector)
    product_dir = os.path.join(bucket, product_name)
    return product_dir


def dt_to_year_doy_hour(dt):
    year = dt.strftime("%Y")  # year
    day_of_year = dt.strftime("%j")  # day of year in julian format
    hour = dt.strftime("%H")  # 2-digit hour format
    return year, day_of_year, hour


####---------------------------------------------------------------------------.
#### Filtering


def _separate_product_scene_abbr(product_scene_abbr):
    "Return (product, scene_abbr) from <product><scene_abbr> string."
    last_letter = product_scene_abbr[-1]
    # Mesoscale domain
    if last_letter in ["1", "2"]:
        return product_scene_abbr[:-2], product_scene_abbr[-2:]
    # CONUS and Full Disc
    elif last_letter in ["F", "C"]:
        return product_scene_abbr[:-1], product_scene_abbr[-1]
    else:
        raise NotImplementedError("Adapat the file patterns.")


def get_info_from_filename(fname, sensor, product_level):
    from goes_api.listing import GLOB_FNAME_PATTERN

    fpattern = GLOB_FNAME_PATTERN[sensor][product_level]
    p = Parser(fpattern)
    info_dict = p.parse(fname)
    # Round start_time and end_time to minute resolution
    info_dict["start_time"] = info_dict["start_time"].replace(microsecond=0, second=0)
    info_dict["end_time"] = info_dict["end_time"].replace(microsecond=0, second=0)
    # Special treatment for identify scene_abbr in L2 products
    if info_dict.get("product_scene_abbr") is not None:
        product, scene_abbr = _separate_product_scene_abbr(
            info_dict.get("product_scene_abbr")
        )
        info_dict["product"] = product
        info_dict["scene_abbr"] = scene_abbr
        del info_dict["product_scene_abbr"]
    # Return info dictionary
    return info_dict


def get_info_from_filepath(fpath, sensor, product_level):
    if not isinstance(fpath, str):
        raise TypeError("'fpath' must be a string.")
    fname = os.path.basename(fpath)
    return get_info_from_filename(fname, sensor, product_level)


def get_key_from_filepaths(fpaths, sensor, product_level, key):
    if isinstance(fpaths, str):
        fpaths = [fpaths]
    return [
        get_info_from_filepath(fpath, sensor, product_level)[key] for fpath in fpaths
    ]


def _filter_file(
    fpath,
    sensor,
    product_level,
    start_time=None,
    end_time=None,
    scan_mode=None,
    channels=None,
    scene_abbr=None,
):
    # scan_mode and channels must be list, start_time and end_time a datetime object

    # Get info from filepath
    info_dict = get_info_from_filepath(fpath, sensor, product_level)

    # Filter by channels
    if channels is not None:
        file_channel = info_dict.get("channel")
        if file_channel is not None:
            if file_channel not in channels:
                return None

    # Filter by scan mode
    if scan_mode is not None:
        file_scan_mode = info_dict.get("scan_mode")
        if file_scan_mode is not None:
            if file_scan_mode not in scan_mode:
                return None

    # Filter by scene_abbr
    if scene_abbr is not None:
        file_scene_abbr = info_dict.get("scene_abbr")
        if file_scene_abbr is not None:
            if file_scene_abbr not in scene_abbr:
                return None

    # Filter by start_time
    if start_time is not None:
        # If the file ends before start_time, do not select
        file_end_time = info_dict.get("end_time")
        if file_end_time <= start_time:
            return None
        # This would exclude a file with start_time within the file
        # if file_start_time < start_time:
        #     return None

    # Filter by end_time
    if end_time is not None:
        file_start_time = info_dict.get("start_time")
        # If the file starts after end_time, do not select
        if file_start_time > end_time:
            return None
        # This would exclude a file with end_time within the file
        # if file_end_time > end_time:
        #     return None
    return fpath


def filter_files(
    fpaths,
    sensor,
    product_level,
    start_time=None,
    end_time=None,
    scan_mode=None,
    channels=None,
    scene_abbr=None,
):
    if isinstance(fpaths, str):
        fpaths = [fpaths]
    fpaths = [
        _filter_file(
            fpath,
            sensor,
            product_level,
            start_time=start_time,
            end_time=end_time,
            scan_mode=scan_mode,
            channels=channels,
            scene_abbr=scene_abbr,
        )
        for fpath in fpaths
    ]
    fpaths = [fpath for fpath in fpaths if fpath is not None]
    return fpaths


####---------------------------------------------------------------------------.
#### Search files
def _get_sector_timedelta(sector):
    if sector == "M":
        dt = datetime.timedelta(minutes=1)
    elif sector == "C":
        dt = datetime.timedelta(minutes=5)
    elif sector == "F":
        dt = datetime.timedelta(minutes=15)  # to include all scan_mode options
    return dt


def group_fpaths_by_key(fpaths, sensor, product_level, key="start_time"):
    list_key_values = [
        get_info_from_filepath(fpath, sensor, product_level)[key] for fpath in fpaths
    ]
    # - Sort fpaths by key values
    idx_key_sorting = np.array(list_key_values).argsort()
    fpaths = np.array(fpaths)[idx_key_sorting]
    list_key_values = np.array(list_key_values)[idx_key_sorting]
    # - Retrieve first occurence of new key value
    unique_key_values, cut_idx = np.unique(list_key_values, return_index=True)
    # - Split by key value
    fpaths_grouped = np.split(fpaths, cut_idx)[1:]
    # - Create (key: files) dictionary
    fpaths_dict = dict(zip(unique_key_values, fpaths_grouped))
    return fpaths_dict


def find_files(
    satellite,
    sensor,
    product_level,
    product,
    sector,
    start_time,
    end_time,
    filter_parameters={},
    group_by_key=None,
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    verbose=False,
):
    # In reading mode, if looking at mesoscale domain, remember to specify scene_abbr == "M<1/2> in  filter_parameters

    # Check inputs
    if protocol is None and base_dir is None:
        raise ValueError("Specify 1 between `base_dir` and `protocol`")
    if base_dir is not None:
        if protocol is not None:
            if protocol not in ["file", "local"]:
                raise ValueError("If base_dir is specified, protocol must be None.")

    # Format inputs
    protocol = check_protocol(protocol)
    base_dir = check_base_dir(base_dir)
    connection_type = check_connection_type(connection_type, protocol)
    satellite = check_satellite(satellite)
    sensor = check_sensor(sensor)
    sector = check_sector(sector)
    product_level = check_product_level(product_level, product=None)
    product = check_product(product, sensor=sensor, product_level=product_level)
    start_time, end_time = check_start_end_time(start_time, end_time)

    filter_parameters = check_filter_parameters(
        filter_parameters, sensor, sector=sector
    )
    group_by_key = check_group_by_key(group_by_key, sensor, product_level)

    # Add start_time and end_time to filter_parameters
    # TODO: could be set to None except for first and last glob pattern !
    filter_parameters["start_time"] = start_time
    filter_parameters["end_time"] = end_time

    # Get filesystem
    fs = get_filesystem(protocol=protocol, fs_args=fs_args)

    bucket_prefix = get_bucket_prefix(protocol)

    # Get product dir
    product_dir = get_product_dir(
        protocol=protocol,
        base_dir=base_dir,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
    )

    # Define time directories
    start_year, start_doy, start_hour = dt_to_year_doy_hour(start_time)
    end_year, end_doy, end_hour = dt_to_year_doy_hour(end_time)
    list_hourly_times = pandas.date_range(start_time, end_time, freq="1h")
    list_year_doy_hour = [dt_to_year_doy_hour(dt) for dt in list_hourly_times]
    list_year_doy_hour = ["/".join(tpl) for tpl in list_year_doy_hour]
    # Define glob patterns
    list_glob_pattern = [
        os.path.join(product_dir, dt_str, "*.nc") for dt_str in list_year_doy_hour
    ]
    n_directories = len(list_glob_pattern)
    if verbose:
        print(f"Searching files across {n_directories} directories.")

    # Loop over each directory [TODO parallel]
    list_fpaths = []
    # glob_pattern = list_glob_pattern[0]
    for glob_pattern in list_glob_pattern:
        # Retrieve list of files
        fpaths = fs.glob(glob_pattern)
        # Add bucket prefix
        fpaths = [bucket_prefix + fpath for fpath in fpaths]
        # Filter files if necessary
        if len(filter_parameters) >= 1:
            fpaths = filter_files(fpaths, sensor, product_level, **filter_parameters)
        list_fpaths += fpaths

    fpaths = list_fpaths

    # Group fpaths by key
    if group_by_key:
        fpaths = group_fpaths_by_key(fpaths, sensor, product_level, key=group_by_key)
    # Parse fpaths for connection type
    fpaths = _set_connection_type(
        fpaths, satellite=satellite, protocol=protocol, connection_type=connection_type
    )
    # Return fpaths
    return fpaths


####---------------------------------------------------------------------------.
#### Search start_time
def find_closest_start_time(
    time,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    filter_parameters={},
):
    # Set time precision to minutes
    time = check_time(time)
    time = time.replace(microsecond=0, second=0)
    # Retrieve timedelta conditioned to sector type
    timedelta = _get_sector_timedelta(sector)
    # Define start_time and end_time
    start_time = time - timedelta
    end_time = time + timedelta
    # Retrieve files
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
        verbose=False,
    )
    # Select start_time closest to time
    list_datetime = sorted(list(fpath_dict.keys()))
    if len(list_datetime) == 0:
        dt_str = int(timedelta.seconds / 60)
        raise ValueError(
            f"No data available in previous and next {dt_str} minutes around {time}."
        )
    idx_closest = np.argmin(np.abs(np.array(list_datetime) - time))
    datetime_closest = list_datetime[idx_closest]
    return datetime_closest


def find_latest_start_time(
    satellite,
    sensor,
    product_level,
    product,
    sector,
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    filter_parameters={},
    look_ahead_minutes=30,
):
    # Search in the past N hour of data
    start_time = datetime.datetime.utcnow() - datetime.timedelta(
        minutes=look_ahead_minutes
    )
    end_time = datetime.datetime.utcnow()
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
        connection_type=connection_type,
        verbose=False,
    )
    # Find the latest time available
    list_datetime = list(fpath_dict.keys())
    idx_latest = np.argmax(np.array(list_datetime))
    datetime_latest = list_datetime[idx_latest]
    return datetime_latest


def find_previous_files(
    start_time,
    N,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    filter_parameters={},
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    include_start_time=False,
    check_consistency=True,
):
    """
    Find files for N timesteps previous to start_time.

    Parameters
    ----------
    start_time : datetime
        The start_time from which search for previous files.
        The start_time should correspond exactly to file start_time if check_consistency=True
    N : int
        The number of previous timesteps for which to retrieve the files.
    include_start_time: bool, optional
        Wheter to include (and count) start_time in the N returned timesteps.
        The default is False.
    check_consistency : bool, optional
        Check for consistency of the returned files. The default is True.
        It check that:
         - start_time correspond exactly to the start_time of the files;
         - the regularity of the previous timesteps, with no missing timesteps;
         - the regularity of the scan mode, i.e. not switching from M3 to M6,
         - if sector == M, the mesoscale domains are not changing within the considered period.

    Returns
    -------
    fpath_dict : dict
        Dictionary with structure {<datetime>: [fpaths]}

    """
    # Set time precision to minutes
    start_time = check_time(start_time)
    start_time = start_time.replace(microsecond=0, second=0)
    # Get closest time and check is as start_time (otherwise warning)
    closest_time = find_closest_start_time(
        time=start_time,
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
    # Check start_time is the precise start_time of the file
    if check_consistency and closest_time != start_time:
        raise ValueError(
            f"start_time='{start_time}' is not an actual start_time. "
            f"The closest start_time is '{closest_time}'"
        )
    # Retrieve timedelta conditioned to sector type
    timedelta = _get_sector_timedelta(sector)
    # Define start_time and end_time
    start_time = closest_time - timedelta * N
    end_time = closest_time
    # Retrieve files
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
        connection_type=connection_type,
        verbose=False,
    )
    # List previous datetime
    list_datetime = sorted(list(fpath_dict.keys()))
    if not include_start_time:
        list_datetime.remove(closest_time)
    list_datetime = sorted(list_datetime)
    # Check data availability
    if len(list_datetime) == 0:
        raise ValueError(f"No data available between {start_time} and {end_time}.")
    if len(list_datetime) < N:
        raise ValueError(
            f"No {N} timesteps available between {start_time} and {end_time}."
        )
    # Select N most recent start_time
    list_datetime = list_datetime[-N:]
    # Select files for N most recent start_time
    fpath_dict = {tt: fpath_dict[tt] for tt in list_datetime}
    # ----------------------------------------------------------
    # Perform consistency checks
    if check_consistency:
        # Check constant scan_mode
        check_unique_scan_mode(fpath_dict, sensor, product_level)
        # Check for interval regularity
        check_interval_regularity(list_datetime + [closest_time])
        # TODO Check for Mesoscale same location (on M1 and M2 separately) !
        # - raise information when it changes !
        if sector == "M":
            pass
    # ----------------------------------------------------------
    # Return files dictionary
    return fpath_dict


def find_next_files(
    start_time,
    N,
    satellite,
    sensor,
    product_level,
    product,
    sector,
    filter_parameters={},
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    include_start_time=False,
    check_consistency=True,
):
    """
    Find files for N timesteps after start_time.

    Parameters
    ----------
    start_time : datetime
        The start_time from which search for next files.
        The start_time should correspond exactly to file start_time if check_consistency=True
    N : int
        The number of next timesteps for which to retrieve the files.
    include_start_time: bool, optional
        Wheter to include (and count) start_time in the N returned timesteps.
        The default is False.
    check_consistency : bool, optional
        Check for consistency of the returned files. The default is True.
        It check that:
         - start_time correspond exactly to the start_time of the files;
         - the regularity of the previous timesteps, with no missing timesteps;
         - the regularity of the scan mode, i.e. not switching from M3 to M6,
         - if sector == M, the mesoscale domains are not changing within the considered period.

    Returns
    -------
    fpath_dict : dict
        Dictionary with structure {<datetime>: [fpaths]}

    """
    # Set time precision to minutes
    start_time = check_time(start_time)
    start_time = start_time.replace(microsecond=0, second=0)
    # Get closest time and check is as start_time (otherwise warning)
    closest_time = find_closest_start_time(
        time=start_time,
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
    # Check start_time is the precise start_time of the file
    if check_consistency and closest_time != start_time:
        raise ValueError(
            f"start_time='{start_time}' is not an actual start_time. "
            f"The closest start_time is '{closest_time}'"
        )
    # Retrieve timedelta conditioned to sector type
    timedelta = _get_sector_timedelta(sector)
    # Define start_time and end_time
    start_time = closest_time
    end_time = closest_time + timedelta * N
    # Retrieve files
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
        connection_type=connection_type,
        verbose=False,
    )
    # List previous datetime
    list_datetime = sorted(list(fpath_dict.keys()))
    if not include_start_time:
        list_datetime.remove(closest_time)
    list_datetime = sorted(list_datetime)
    # Check data availability
    if len(list_datetime) == 0:
        raise ValueError(f"No data available between {start_time} and {end_time}.")
    if len(list_datetime) < N:
        raise ValueError(
            f"No {N} timesteps available between {start_time} and {end_time}."
        )
    # Select N most recent start_time
    list_datetime = list_datetime[0:N]
    # Select files for N most recent start_time
    fpath_dict = {tt: fpath_dict[tt] for tt in list_datetime}
    # ----------------------------------------------------------
    # Perform consistency checks
    if check_consistency:
        # Check constant scan_mode
        check_unique_scan_mode(fpath_dict, sensor, product_level)
        # Check for interval regularity
        check_interval_regularity(list_datetime + [closest_time])
        # TODO Check for Mesoscale same location (on M1 and M2 separately) !
        # - raise information when it changes !
        if sector == "M":
            pass
    # ----------------------------------------------------------
    # Return files dictionary
    return fpath_dict


####--------------------------------------------------------------------------.
#### Output options
def _add_nc_mode_bytes(fpaths):
    """Add `#mode=bytes` to the HTTP netCDF4 url."""
    fpaths = [fpath + "#mode=bytes" for fpath in fpaths]
    return fpaths


def _set_connection_type(fpaths, satellite, protocol=None, connection_type=None):
    """Switch from bucket to https connection for protocol 'gcs' and 's3'."""
    if protocol is None:
        return fpaths
    if protocol == "file":
        return fpaths
    # here protocol gcs or s3
    if connection_type == "bucket":
        return fpaths
    if connection_type in ["https", "nc_bytes"]:
        if isinstance(fpaths, list):
            fpaths = _switch_to_https_fpaths(
                fpaths, protocol=protocol, satellite=satellite
            )
            if connection_type == "nc_bytes":
                fpaths = _add_nc_mode_bytes(fpaths)
        if isinstance(fpaths, dict):
            fpaths = {
                tt: _switch_to_https_fpaths(
                    l_fpaths, protocol=protocol, satellite=satellite
                )
                for tt, l_fpaths in fpaths.items()
            }
            if connection_type == "nc_bytes":
                fpaths = {
                    tt: _add_nc_mode_bytes(l_fpaths) for tt, l_fpaths in fpaths.items()
                }
        return fpaths
    else:  # TODO: add kerchunk (maybe)
        raise NotImplementedError(
            "'bucket','https', 'nc_bytes' are the only `connection_type` available."
        )


####--------------------------------------------------------------------------.
#### Utils
import subprocess
import sys


def open_directory_explorer(satellite, protocol=None, base_dir=None):
    import webbrowser

    if protocol == "s3":
        satellite = satellite.replace("-", "")  # goes16
        fpath = f"https://noaa-{satellite}.s3.amazonaws.com/index.html"
        webbrowser.open(fpath, new=1)
    elif protocol == "gcs":
        # goes-16
        fpath = f"https://console.cloud.google.com/storage/browser/gcp-public-data-{satellite}"
        webbrowser.open(fpath, new=1)
    elif base_dir is not None:
        webbrowser.open(os.path.join(base_dir, satellite))
    else:
        raise NotImplementedError(
            "Current available protocols are 'gcs', 's3', 'local'."
        )


def get_ABI_channel_info(channel):
    "Open ABI QuickGuide of the channel."
    # http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/
    import webbrowser

    channel = check_channel(channel)
    channel_number = channel[1:]  # 01-16
    url = f"http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band{channel_number}.pdf"
    webbrowser.open(url, new=1)
    return None


# TODO: FOR ABI L2
# http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/


def get_available_online_product(protocol, satellite):
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
