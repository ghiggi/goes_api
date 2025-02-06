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

import datetime

import goes_api

goes_api.open_explorer(satellite="goes16", protocol="s3")
goes_api.open_explorer(satellite="goes16", protocol="gcs")
goes_api.open_explorer(satellite="goes16", base_dir="/ltenas3/data/")


protocol = "s3"
satellite = "goes-16"
product = "ACHA"
product_level = "L2"
sensor = "ABI"
sector = "F"
time = datetime.datetime(2022, 3, 16, 15, 30)


goes_api.open_explorer_dir(
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    time=time,
    protocol=protocol,
)


goes_api.open_abi_channel_guide("C01")
goes_api.open_abi_product_guide("CTP")
goes_api.open_abi_product_guide("RRQPE")
