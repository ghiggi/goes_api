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


PROTOCOLS = ["gcs", "s3", "local", "file"]


BUCKET_PROTOCOLS = ["gcs", "s3"]


_satellites = {
    "goes-16": ["16", "G16", "GOES-16", "GOES16"],
    "goes-17": ["17", "G17", "GOES-17", "GOES17"],
    "goes-18": ["18", "G18", "GOES-18", "GOES18"],
}


_sectors = {
    "F": ["FULL", "FULLDISK", "FULL DISK", "F", "FLDK"],
    "C": ["CONUS", "PACUS", "C", "P"],
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
    "C08": [
        "C08",
        "8",
        "08",
        "6.2",
        "UPPER-LEVEL TROPOSPHERIC WATER VAPOUR",
        "UPPER-LEVEL WATER VAPOUR",
    ],
    "C09": [
        "C09",
        "9",
        "09",
        "6.9",
        "MID-LEVEL TROPOSPHERIC WATER VAPOUR",
        "MID-LEVEL WATER VAPOUR",
    ],
    "C10": [
        "C10",
        "10",
        "10",
        "7.3",
        "LOWER-LEVEL TROPOSPHERIC WATER VAPOUR",
        "LOWER-LEVEL WATER VAPOUR",
    ],
    "C11": ["C11", "11", "11", "8.4", "CLOUD-TOP PHASE", "CTP"],
    "C12": ["C12", "12", "12", "9.6", "OZONE"],
    "C13": ["C13", "13", "10.3", "CLEAN IR LONGWAVE WINDOW", "CLEAN IR"],
    "C14": ["C14", "14", "11.2", "IR LONGWAVE WINDOW", "IR LONGWAVE"],
    "C15": ["C15", "15", "12.3", "DIRTY LONGWAVE WINDOW", "DIRTY IR"],
    "C16": ["C16", "16", "13.3", "CO2 IR LONGWAVE", "CO2", "CO2 IR"],
}

