#!/usr/bin/env python3
"""
Created on Thu Mar  9 11:46:07 2023

@author: ghiggi
"""
import os
from typing import Dict

import yaml


def _read_yaml_file(fpath):
    """Read a YAML file into dictionary."""
    with open(fpath) as f:
        dictionary = yaml.safe_load(f)
    return dictionary


def _write_yaml_file(dictionary, fpath, sort_keys=False):
    """Write dictionary to YAML file."""
    with open(fpath, "w") as f:
        yaml.dump(dictionary, f, sort_keys=sort_keys)


def define_goes_api_configs(base_dir: str):
    """
    Defines the GOES-API configuration file with the given credentials and base directory.

    Parameters
    ----------
    base_dir : str
        The base directpry where GOES data are stored.

    Notes
    -----
    This function writes a YAML file to the user's home directory at ~/.config_goes_api.yml
    with the given GOES-API credentials and base directory. The configuration file can be
    used for authentication when making GOES-API requests.

    """
    # TODO:
    # - add preferred cloud protocol
    # - add other fs cloud options

    config_dict = {}
    config_dict["base_dir"] = base_dir

    # Retrieve user home directory
    home_directory = os.path.expanduser("~")

    # Define path to .config_goes_api.yaml file
    fpath = os.path.join(home_directory, ".config_goes_api.yml")

    # Write the config file
    _write_yaml_file(config_dict, fpath, sort_keys=False)

    print("The GOES-API config file has been written successfully!")


def read_goes_api_configs() -> Dict[str, str]:
    """
    Reads the GOES-API configuration file and returns a dictionary with the configuration settings.

    Returns
    -------
    dict
        A dictionary containing the configuration settings for the GOES-API.

    Raises
    ------
    ValueError
        If the configuration file has not been defined yet. Use `goes_api.define_configs()` to
        specify the configuration file path and settings.

    Notes
    -----
    This function reads the YAML configuration file located at ~/.config_goes_api.yml, which
    should contain the GOES-API credentials and base directory specified by `goes_api.define_configs()`.
    """
    # Retrieve user home directory
    home_directory = os.path.expanduser("~")
    # Define path where .config_goes_api.yaml file should be located
    fpath = os.path.join(home_directory, ".config_goes_api.yml")
    if not os.path.exists(fpath):
        raise ValueError(
            "The GOES-API config file has not been specified. Use goes_api.define_configs to specify it !",
        )
    # Read the GOES-API config file
    config_dict = _read_yaml_file(fpath)
    return config_dict


####--------------------------------------------------------------------------.
def _get_config_key(key, value=None):
    """Return the config key if `value` is None."""
    if value is None:
        value = read_goes_api_configs()[key]
    return value


def get_goes_base_dir(base_dir=None):
    """Return the GOES base directory."""
    return _get_config_key(key="base_dir", value=base_dir)
