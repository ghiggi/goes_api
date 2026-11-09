## Operational events

GOES-16 (GOES-R)

- November 30 - December 11 2017: Drifting from 89.5 °W to the GOES-East operational location (75.2° W) [todo: check if 89.5 is correct]
- Nominal operation resumed on December 18, 2017
- Declared GOES-EAST on December 18, 2017
- Biased IR bands before June 19, 2018

GOES-13

- Turned off on January 8, 2018 (replaced by GOES-16)

GOES-17 (GOES-S)

- Launched on March 1, 2018
- October 24 - November 13, 2018: Drifting from XXXX °W to the GOES-West operational location (137.2° W)
- Nominal operations resumed on November 15, 2018
- Declared GOES-West on February 12, 2019

GOES-18 Transition

- https://www.ospo.noaa.gov/Operations/GOES/transition.html
- https://www.goes-r.gov/users/transitionToOperations18.html

GOES ReBroadcast (GRB)

ABI intro

- https://www.goes-r.gov/featureStories/transformingEnergy.html
- https://www.goes-r.gov/spacesegment/abi.html
- https://rammb.cira.colostate.edu/training/visit/training_sessions/basic_operations_of_abi_on_goes_r/old_video/presentation_html5.html

ABI bands and composites

- http://cimss.ssec.wisc.edu/goes/GOESR_QuickGuides.html
- https://rammb.cira.colostate.edu/training/visit/quick_guides/

Training material

- https://rammb2.cira.colostate.edu/training/visit/training_sessions/
- https://www.ssec.wisc.edu/~scottl/SHyMet/

Status

- https://qcweb.ssec.wisc.edu/web/abi_quality_scores/
- https://www.ospo.noaa.gov/Operations/GOES/status.html
- http://cimss.ssec.wisc.edu/goes-r/abi-/band_statistics_imagery.html
- https://www.goes-r.gov/users/GOES-17-ABI-Performance.html

Navigation and Registration Status

- https://www.ospo.noaa.gov/Operations/GOES/goes-inrstats.html

Weekly Operations Plan

- https://noaasis.noaa.gov/cemscs/goeswkly.txt

Calibration event log

- https://www.star.nesdis.noaa.gov/GOESCal/goes_SatelliteAnomalies.php

Precomputed angles/latlon

- https://www.star.nesdis.noaa.gov/pub/smcd/spb/fwu/tmp/latlon_angle/ (lon=-135.0 deg) and East (lon=-75.0)

GOES weighting functions

- https://cimss.ssec.wisc.edu/goes/wf/

Imagery

- RAMB Slider: http://rammb-slider.cira.colostate.edu/
- SSEC GeoSphere: https://geosphere.ssec.wisc.edu/#coordinate:0,0;
- SSEC Geo Browser: https://www.ssec.wisc.edu/data/geo/#/animation?satellite=goes-16-17-comp
- SSEC Real Erth: https://re.ssec.wisc.edu/?products=G16-ABI-FD-BAND02.100,G16-ABI-FD-BAND13.65&center=36.62792196407514,-94.74951171874999&zoom=4&width=949&height=776&timeproduct=G16-ABI-FD-BAND13&timespan=-6t&animationspeed=50&labels=lines
- Zoom Earth: https://zoom.earth/#view=37.4,-116.1,4z/date=2022-04-14,16:20,+2
- GOES Image Viewer: https://www.star.nesdis.noaa.gov/GOES/index.php
- Static and Time-Difference Imagery: http://cimss.ssec.wisc.edu/goes-r/abi-/static_and_timediff_imagery.html

Blogs & Videos

- https://satlib.cira.colostate.edu/
- https://rammb.cira.colostate.edu/ramsdis/online/loop_of_the_day/

Other infos

- http://cimss.ssec.wisc.edu/goes/goesdata.html

GOES Satellites

- https://spaceflight101.com/goes-r/goes-r-instruments/

GOES Special Events

- Keep out Zones: https://cimss.ssec.wisc.edu/satellite-blog/archives/27662

## GOES L1B ABI FILE SIZE STATISTICS

GOES16 dataset chunks:

| Sector    | CHUNK SHAPE | CHUNK MEMORY (float32) | CHUNK DISK (int16)    |
| --------- | ----------- | ---------------------- | --------------------- |
| Full Disc | (226,226)   | 0.19 MB                | 99 KB (uncompressed)  |
| CONUS     | (250,250)   | 0.23 MB                | 122 KB (uncompressed) |
| Mesoscale | (250,250)   | 0.23 MB                | 122 KB (uncompressed) |

GOES16 Full Disc array:

| Resolution  | ARRAY SHAPE    | ARRAY MEMORY (float32) | # CHUNKS |
| ----------- | -------------- | ---------------------- | -------- |
| 500 m (C02) | (21696, 21696) | 1.75 GB                | 9216     |
| 1 km        | (10848,10848)  | 449 MB                 | 2304     |
| 2 km        | (5424, 5424)   | 112 MB                 | 576      |

GOES16 CONUS array:

| Resolution  | ARRAY SHAPE   | ARRAY MEMORY (float32) | # CHUNKS |
| ----------- | ------------- | ---------------------- | -------- |
| 500 m (C02) | (6000, 10000) | 228 MB                 | 960      |
| 1 km        | (3000, 5000)  | 57 MB                  | 240      |
| 2 km        | (1500, 2500)  | 14 MB                  | 60       |

GOES16 ABI all channels (average) daily disk storage amount

| Sector    | Mode 3 | Mode 6 |
| --------- | ------ | ------ |
| Full Disc |        | GB     |
| CONUS     |        | GB     |
| Mesoscale |        | GB     |

Other notes

- Chunks are compressed with zlib and a deflation level of 1. No bytes shuffle is applied to the arrays.
- Full Disc arrays have 21 % of pixels of unvalid (nan) pixels.

## ABI Radiance products

The L1b Radiances and L2 Cloud and Moisture Imagery (CMIP) products have separate netCDF files for each of the 16 spectral bands.
The L2 Cloud and Moisture Imagery (MCMIP) products contains all the 16 spectral bands into a single file.

## ABI Raw Data

Data are saved as 16-bit scaled integers, rather than 32-bit floating point values.
To unpack: unpacked_value = packed_value * scale_factor + add_offset
To pack: packed_value = (unpacked_value - add_offset) / scale_factor
--> The scale factor is calculated with the formula (Max Value - Min Value)/65530

## ABI Data Processing

- For reflective bands (1-6), convert to reflectance.
- For the emissive bands (7-16), convert to brightness temperature (Kelvin).
- It is recommended to always use the coefficients embedded in the file metadata
  for conversion to reflectance or brightness temperature.

## ABI Data quality flags (DQFs)

- Quality scores: https://qcweb.ssec.wisc.edu/web/abi_quality_scores/

## ABI Cloud products

- https://www.star.nesdis.noaa.gov/goesr/product_cp_cloud.php

## Cloud Bucket Products

- AWS: - https://docs.opendata.aws/noaa-goes16/cics-readme.html

## Other python packages for GOES ABI download

- [goes2go](https://github.com/blaylockbk/goes2go) (Note: has a nice documentation)
- [goesaws](https://github.com/mnichol3/goesaws) (Note: has a simple CLI)
- [goes-downloader](https://github.com/uba/goes-downloader) (Note: has a GUI)
- [goesmirror](https://github.com/meteoswiss-mdr/goesmirror/blob/master/goesmirror/goesmirror.py)
- [goes-py](https://github.com/palexandremello/goes-py)
- [GOES](https://github.com/joaohenry23/GOES)

# Orthorectifying

https://github.com/spestana/goes-ortho

## GOES Products & Useful links

https://www.goes-r.gov/products/overview.html
https://rammb.cira.colostate.edu/training/visit/quick_guides/
https://blaylockbk.github.io/goes2go/_build/html/#useful-links

## Cloud buckets explorer

Google Cloud Storage

- https://console.cloud.google.com/storage/browser/gcp-public-data-goes-17
- https://console.cloud.google.com/storage/browser/gcp-public-data-goes-16

AWS S3 (us-east-1 region)

- https://noaa-goes17.s3.amazonaws.com/index.html
- https://noaa-goes16.s3.amazonaws.com/index.html

## Other cloud storage

Oracle cloud storage

- https://opendata.oraclecloud.com/ords/r/opendata/opendata/details?data_set_id=2&clear=CR,8&session=2142808942446

Microsoft West Europe Azure Blob Storage (only limited products)

- https://planetarycomputer.microsoft.com/dataset/goes-cmi
- https://planetarycomputer.microsoft.com/dataset/goes-cmi#Storage-Documentation
- https://github.com/microsoft/AIforEarthDataSets/blob/main/data/goes-r.md#storage-resources
- https://planetarycomputer.microsoft.com/dataset/goes-cmi#Blob-Storage-Notebook
- https://goeseuwest.blob.core.windows.net/noaa-goes16
- https://goeseuwest.blob.core.windows.net/noaa-goes17

## fsspec

- [fsspec](https://github.com/fsspec)
- [s3fs](https://s3fs.readthedocs.io/en/latest/) (via boto3)
- [gcsfs](https://gcsfs.readthedocs.io/en/latest/index.html)
- [adlfs](https://github.com/fsspec/adlfs) (via azure.storage.blob ???)

## Example cloud bucket url

- "gs://gcp-public-data-goes-16/ABI-L1b-RadF/2017/258/18/OR_ABI-L1b-RadF-M3C14_G16_s20172581800377_e20172581811144_c20172581811208.nc"
- "s3://noaa-goes16/ABI-L1b-RadF/2019/321/14/OR_ABI-L1b-RadF-M6C01_G16_s20193211440283_e20193211449591_c20193211450047.nc"

## Grant and free trial info

- https://www.ncei.noaa.gov/access/cloud-access

## Other interesting dataset

NOAA Global Hydro Estimator (GHE)

- https://microsoft.github.io/AIforEarthDataSets/data/ghe.html
- https://registry.opendata.aws/noaa-ghe/

GHCN-D

- https://registry.opendata.aws/noaa-ghcn/
- https://console.cloud.google.com/marketplace/product/noaa-public/ghcn-d?filter=solution-type:dataset&q=NOAA&id=9d500d1d-fda4-4413-a789-d8786fd6592e&pli=1

## GEO imagery

- https://satepsanone.nesdis.noaa.gov/

## THREDDS Catalogs

- https://www.unidata.ucar.edu/software/mcidas/adde_servers.html (lot of link)

Unidata THREDDS - GOES-R Data of last 14 days

- https://thredds-test.unidata.ucar.edu/thredds/catalog/satellite/goes16/GOES17/catalog.html
- goeseast.unidata.ucar.edu
- goeswest.unidata.ucar.edu

NCEI THREDDS - GridSat (Global 3-hourly IR from 1980 to 2021)

- 8 km resolution at equator
- Equal area grid is 0.07 degrees latitude
- From -70 to 70 N
- https://www.ncei.noaa.gov/thredds/catalog/cdr/gridsat/catalog.html

NCEI THREDDS - PERSIANN-CDR

- https://www.ncei.noaa.gov/thredds/catalog/cdr/persiann/catalog.html

Other NCEII data

- https://www.ncei.noaa.gov/data/
