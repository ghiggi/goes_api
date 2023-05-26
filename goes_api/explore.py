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
import webbrowser
from goes_api.configs import get_goes_base_dir
from goes_api.checks import (
    _check_satellite,
    _check_channel,
    _check_product,
)
from goes_api.io import _get_product_name, _dt_to_year_doy_hour


def _get_sat_explorer_path(protocol, satellite, base_dir=None):
    """Return the path to the satellite directory."""
    if protocol == "s3":
        satellite = satellite.replace("-", "")  # goes16
        path = f"https://noaa-{satellite}.s3.amazonaws.com/index.html" 
    elif protocol == "gcs":
        # goes-16
        path = f"https://console.cloud.google.com/storage/browser/gcp-public-data-{satellite}"
    elif protocol in ["file", "local"]:
        # Get default local directory if not specified
        base_dir = get_goes_base_dir(base_dir)
        path = os.path.join(base_dir, satellite)
    else:
        raise NotImplementedError(
            "Current available protocols are ['gcs','s3','local']"
        )
    return path 


      
def open_explorer(satellite, protocol="s3", base_dir=None):
    """Open the cloud bucket / local explorer into a webpage.

    Parameters
    ----------
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    protocol : str, optional
        String specifying the cloud bucket storage that you want to explore.
        Use `goes_api.available_protocols()` to retrieve available protocols.
        The default is "s3".
    base_dir : str, optional
        Local base directory path where GOES data are stored.
        This argument must be specified only if wanting to explore the local storage.
        If protocol="file" and base_dir is None, base_dir is retrieved from 
        the GOES-API config file.
    """
    satellite = _check_satellite(satellite)
    path = _get_sat_explorer_path(protocol=protocol, satellite=satellite, base_dir=base_dir)
    webbrowser.open(path, new=1)
    

def open_explorer_dir(
    satellite, 
    sensor, 
    product_level, 
    product, 
    time, 
    sector=None, 
    protocol="s3", base_dir=None
    ):
    """Open the cloud bucket / local exlorer at the doy-hourly directory into a webpage.

    Parameters
    ----------
    satellite : str
        The name of the satellite.
        Use `goes_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `goes_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `goes_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `goes_api.available_products()` for a list of available products.
    time : datetime.datetime
        Time at which you want to explore file content.
    sector : str, optional
        The acronym of the ABI sector for which to retrieve the files.
        See `goes_api.available_sectors()` for a list of available sectors.
    protocol : str, optional
        String specifying the cloud bucket storage that you want to explore.
        Use `goes_api.available_protocols()` to retrieve available protocols.
        The default is "s3".
    base_dir : str, optional
        Local base directory path where GOES data are stored.
        This argument must be specified only if wanting to explore the local storage.
        If protocol="file" and base_dir is None, base_dir is retrieved from 
        the GOES-API config file.

    """    
    satellite = _check_satellite(satellite)
    year, doy, hour = _dt_to_year_doy_hour(time)
    product_name = _get_product_name(sensor=sensor, 
                                     product_level=product_level, 
                                     product=product, 
                                     sector=sector) 
    path = _get_sat_explorer_path(protocol=protocol, satellite=satellite, base_dir=base_dir)      
    if protocol == "s3":
        fpath = path + f"#{product_name}/{year}/{doy}/{hour}/" 
    elif protocol == "gcs":
        fpath = path + f"/{product_name}/{year}/{doy}/{hour}"  
    elif protocol in ["file", "local"]:
        fpath = os.path.join(path, product_name, year, doy, hour)
    else:
        raise NotImplementedError(
            "Current available protocols are 'gcs', 's3', 'local'."
        )
    webbrowser.open(fpath, new=1)
    

def open_abi_channel_guide(channel):
    """Open ABI QuickGuide of the channel.

    See `goes_api.available_channels()` for available ABI channels.
    Source of information: http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/
    """
    import webbrowser

    if not isinstance(channel, str):
        raise TypeError("Expecting a string defining a single channel.")
    channel = _check_channel(channel)
    channel_number = channel[1:]  # 01-16
    url = f"http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band{channel_number}.pdf"
    webbrowser.open(url, new=1)
    return None


def open_abi_product_guide(product):
    """Open ABI QuickGuide of L2 products.

    See `goes_api.available_product(sensors="ABI", product_level="L2")` for available ABI L2 products.
    Source of information: http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/
    """
    import webbrowser

    dict_product_fname = {
        "ACHA": "ABIQuickGuide_BaselineCloudTopHeight.pdf",
        "ACHT": "ABIQuickGuide_BaselineCloudTopTemperature.pdf",
        "ACM": "ABIQuickGuide_BaselineClearSkyMask.pdf",
        "ACTP": "ABIQuickGuide_BaselineCloudPhase.pdf",
        "ADP": "ABIQuickGuide_BaselineAerosolDetection.pdf",
        "AICE": "JPSSQuickGuide_Ice_Concentration_2022.pdf",
        # "AITA": "Ice Thickness and Age",  # only F
        "AOD": "ABIQuickGuide_BaselineAerosolOpticalDepth.pdf",
        # "BRF": "Land Surface Bidirectional Reflectance Factor",
        # "CMIP": "Cloud and Moisture Imagery",
        "COD": "ABIQuickGuide_BaselineCloudOpticalDepth.pdf",
        "CPS": "ABIQuickGuide_BaselineCloudParticleSizeDistribution.pdf	",
        "CTP": "ABIQuickGuide_BaselineCloudTopPressure.pdf",
        "DMW": "ABIQuickGuide_BaselineDerivedMotionWinds.pdf",
        "DMWV": "ABIQuickGuide_BaselineDerivedMotionWinds.pdf",
        "DSI": "ABIQuickGuide_BaselineDerivedStabilityIndices.pdf",
        # "DSR": "Downward Shortwave Radiation",
        "FDC": "QuickGuide_GOESR_FireHotSpot_v2.pdf",
        # "LSA": "Land Surface Albedo",
        "LST": "QuickGuide_GOESR_LandSurfaceTemperature.pdf",
        "LST2KM": "QuickGuide_GOESR_LandSurfaceTemperature.pdf",
        # "LVMP": "Legacy Vertical Moisture Profile",
        # "LVTP": "Legacy Vertical Temperature Profile",
        # "MCMIP": "Cloud and Moisture Imagery (Multi-band format)",
        # "RRQPE": "Rainfall Rate (QPE)",
        # "RSR": "Reflected Shortwave Radiation at TOA",
        # "SST": "Sea Surface (Skin) Temperature",
        # "TPW": "Total Precipitable Water",
        "VAA": "QuickGuide_GOESR_VolcanicAsh.pdf",
    }
    available_products = list(dict_product_fname.keys())
    # Check product
    if not isinstance(product, str):
        raise TypeError("Expecting a string defining a single ABI L2 product.")
    product = _check_product(product=product, sensor="ABI", product_level="L2")
    # Check QuickGuide availability
    fname = dict_product_fname.get(product, None)
    if fname is None:
        raise ValueError(f"No ABI QuickGuide available for product '{product}' .\n" +
                         f"Documentation is available for the following L2 products {available_products}.")
    # Define url and open quickquide
    url = f"http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/{fname}"
    webbrowser.open(url, new=1)

    return None
