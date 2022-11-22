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

import goes_api

goes_api.open_directory_explorer(satellite="goes16", protocol="s3")
goes_api.open_directory_explorer(satellite="goes16", protocol="gcs")
goes_api.open_directory_explorer(satellite="goes16", base_dir="/ltenas3/data/")

goes_api.open_abi_channel_guide("C01")
goes_api.open_abi_product_guide("CTP")
goes_api.open_abi_product_guide("RRQPE")
