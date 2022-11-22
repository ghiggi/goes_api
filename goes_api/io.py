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
"""Define filesystems, buckets, connection types and directory structures"""

import os
import fsspec
import datetime 
import pandas as pd 

 
def get_filesystem(protocol, fs_args={}):
    """
    Define ffspec filesystem.

    protocol : str
       String specifying the cloud bucket storage from which to retrieve
       the data. It must be specified if not searching data on local storage.
       Use `goes_api.available_protocols()` to retrieve available protocols.
    fs_args : dict, optional
       Dictionary specifying optional settings to initiate the fsspec.filesystem.
       The default is an empty dictionary. Anonymous connection is set by default.

    """
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
    """
    Get the cloud bucket address for a specific satellite.

    Parameters
    ----------
    protocol : str
         String specifying the cloud bucket storage from which to retrieve
         the data. Use `goes_api.available_protocols()` to retrieve available protocols.
    satellite : str
        The acronym of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    """

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


def _switch_to_https_fpath(fpath, protocol): 
    """
    Switch bucket address with https address.

    Parameters
    ----------
    fpath : str
        A single bucket filepaths.
    protocol : str
         String specifying the cloud bucket storage from which to retrieve
         the data. Use `goes_api.available_protocols()` to retrieve available protocols.
    """
    from goes_api.info import infer_satellite_from_path
    
    # GCS note
    # https://storage.googleapis.com (download and byte connection)
    # https://storage.cloud.google.com (just download)
    satellite = infer_satellite_from_path(fpath)
    https_base_url_dict = {
        "gcs": "https://storage.googleapis.com/gcp-public-data-{}".format(satellite),
        "s3": "https://noaa-{}.s3.amazonaws.com".format(satellite.replace("-", "")),
    }
    base_url = https_base_url_dict[protocol]
    fpath = os.path.join(base_url, fpath.split("/", 3)[3])  
    return fpath 
    

def _switch_to_https_fpaths(fpaths, protocol):
    """
    Switch bucket address with https address.

    Parameters
    ----------
    fpaths : list
        List of bucket filepaths.
    protocol : str
         String specifying the cloud bucket storage from which to retrieve
         the data. Use `goes_api.available_protocols()` to retrieve available protocols.
    """
    fpaths = [_switch_to_https_fpath(fpath, protocol) for fpath in fpaths]
    return fpaths


def _get_bucket_prefix(protocol):
    """Get protocol prefix."""
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


def _get_product_name(sensor, product_level, product, sector):
    """Get bucket directory name of a product."""
    if sensor=='ABI':
        product_name = f"{sensor}-{product_level}-{product}{sector}"
    else: 
        product_name = f"{sensor}-{product_level}-{product}"
    return product_name

def _get_product_dir(
    satellite, sensor, product_level, product, sector, protocol=None, base_dir=None
):
    """Get product (bucket) directory path."""
    if base_dir is None:
        bucket = get_bucket(protocol, satellite)
    else:
        bucket = os.path.join(base_dir, satellite.upper())
        if not os.path.exists(bucket):
            raise OSError(f"The directory {bucket} does not exist.")
    product_name = _get_product_name(sensor, product_level, product, sector)
    product_dir = os.path.join(bucket, product_name)
    return product_dir


def _dt_to_year_doy_hour(dt):
    """Return the (YYYY, DOY, HH) tuple for a specific datetime object."""
    year = dt.strftime("%Y")  # year
    day_of_year = dt.strftime("%j")  # day of year in julian format
    hour = dt.strftime("%H")  # 2-digit hour format
    return year, day_of_year, hour


def _get_time_dir_tree(start_time, end_time):
    """Define the directory tree based on start and end times.
    
    Pattern: YYYY/DOY/HH
    """
    list_hourly_times = pd.date_range(start_time, 
                                      end_time + datetime.timedelta(hours=1), 
                                      freq="1h")
    list_year_doy_hour = [_dt_to_year_doy_hour(dt) for dt in list_hourly_times]
    list_dir_tree = ["/".join(tpl) for tpl in list_year_doy_hour]
    return list_dir_tree


def _add_nc_bytes(fpaths):
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
            fpaths = _switch_to_https_fpaths(fpaths, protocol=protocol)
            if connection_type == "nc_bytes":
                fpaths = _add_nc_bytes(fpaths)
        if isinstance(fpaths, dict):
            fpaths = {
                tt: _switch_to_https_fpaths(l_fpaths, protocol=protocol)           
                for tt, l_fpaths in fpaths.items()
            }
            if connection_type == "nc_bytes":
                fpaths = {
                    tt: _add_nc_bytes(l_fpaths) for tt, l_fpaths in fpaths.items()
                }
        return fpaths
    else:
        raise NotImplementedError(
            "'bucket','https', 'nc_bytes' are the only `connection_type` available."
        )
        
    