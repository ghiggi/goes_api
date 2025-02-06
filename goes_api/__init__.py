#!/usr/bin/env python3

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

from goes_api.configs import define_goes_api_configs as define_configs
from goes_api.configs import read_goes_api_configs as read_configs
from goes_api.download import (
    download_closest_files,
    download_daily_files,
    download_files,
    download_latest_files,
    download_monthly_files,
    download_next_files,
    download_previous_files,
)
from goes_api.explore import (
    open_abi_channel_guide,
    open_abi_product_guide,
    open_explorer,
    open_explorer_dir,
)
from goes_api.filter import filter_files
from goes_api.info import (
    available_channels,
    available_connection_types,
    available_group_keys,
    available_product_levels,
    available_products,
    available_protocols,
    available_satellites,
    available_scan_modes,
    available_sectors,
    available_sensors,
    get_available_online_product,
    group_files,
)
from goes_api.operations import (
    ensure_fixed_scan_mode,
    ensure_operational_data,
    ensure_time_period_is_covered,
)
from goes_api.search import (
    find_closest_files,
    find_closest_start_time,
    find_files,
    find_latest_files,
    find_latest_start_time,
    find_next_files,
    find_previous_files,
)

__all__ = [
    "define_configs",
    "read_configs",
    "available_protocols",
    "available_sensors",
    "available_satellites",
    "available_sectors",
    "available_product_levels",
    "available_products",
    "available_scan_modes",
    "available_channels",
    "available_connection_types",
    "available_group_keys",
    "get_available_online_product",
    "download_files",
    "download_closest_files",
    "download_latest_files",
    "download_next_files",
    "download_previous_files",
    "download_daily_files",
    "download_monthly_files",
    "find_files",
    "find_latest_files",
    "find_closest_files",
    "find_previous_files",
    "find_next_files",
    "find_closest_start_time",
    "find_latest_start_time",
    "group_files",
    "ensure_operational_data",
    "ensure_data_availability",
    "ensure_regular_timesteps",
    "filter_files",
    "generate_kerchunk_files",
    "open_explorer",
    "open_explorer_dir",
    "open_abi_channel_guide",
    "open_abi_product_guide",
]
