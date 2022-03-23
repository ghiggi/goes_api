## GOES ABI SCAN MODES 

GOES ABI has currently 3 types of `scan_modes`.

Mode 3 (GOES16, GOES17)
- Till April 2, 2019 
- FULL DISK every 15 minutes 
- CONUS every 5 minutes
- Mesoscale every 1 minutes  (TODO: when 30 seconds)

Mode 3 GOES17 Cooling Tim 
- During some periods of the year
- CONUS not scanned 
- Mesoscale M1 and M2 every 2 minutes

Mode 6 (difference between GOES-16  and GOES-17)
- Since  April 2, 2019 
- FULL DISK every 10 minutes 
- CONUS every 5 minutes
- Mesoscale every 1 minutes  (TODO: when 30 seconds)

Mode 4 
- Continuous 5-minute full disk imagery 
- CONUS and Mesoscale products are not available
- Example dates: `datetime.datetime(2018,10,1, ...,...,...) # TODO`

GOES 16 and 17 ABI scan mode timetable are available [here](https://www.goes-r.gov/users/abiScanModeInfo.html)

A video of GOES ABI scan strategy is available [here](shttps://www.youtube.com/watch?v=qCAPwgQR13w&ab_channel=NOAASatellites)

## GOES L1B ABI FILE SIZE STATISTICS 

GOES16 dataset chunks:

| Sector    | CHUNK SHAPE | CHUNK MEMORY (float32) | CHUNK DISK (int16)    |
|-----------|-------------|------------------------|-----------------------|
| Full Disc | (226,226)   | 0.19 MB                | 99 KB (uncompressed)  |
| CONUS     | (250,250)   | 0.23 MB                | 122 KB (uncompressed) |
| Mesoscale | (250,250)   | 0.23 MB                | 122 KB (uncompressed) |

GOES16 Full Disc array: 

| Resolution  | ARRAY SHAPE    | ARRAY MEMORY (float32) | # CHUNKS |
|-------------|----------------|------------------------|----------|
| 500 m (C02) | (21696, 21696) | 1.75 GB                | 9216     |
| 1 km        | (10848,10848)  | 449 MB                 | 2304     |
| 2 km        | (5424, 5424)   | 112 MB                 | 576      |

GOES16 CONUS array:

| Resolution  | ARRAY SHAPE   | ARRAY MEMORY (float32) | # CHUNKS |
|-------------|---------------|------------------------|----------|
| 500 m (C02) | (6000, 10000) | 228 MB                 | 960      |
| 1 km        | (3000, 5000)  | 57 MB                  | 240      |
| 2 km        | (1500, 2500)  | 14 MB                  | 60       |

GOES16 ABI all channels (average) daily disk storage amount 

| Sector    | Mode 3 | Mode 6  |  
|-----------|--------|---------| 
| Full Disc |        |      GB | 
| CONUS     |        |      GB | 
| Mesoscale |        |      GB | 

Other notes
- Chunks are compressed with zlib and a deflation level of 1. No bytes shuffle is applied to the arrays.
- Full Disc arrays have 21 % of pixels of unvalid (nan) pixels. 


## ABI Radiance products 

The L1b Radiances and L2 Cloud and Moisture Imagery (CMIP) products have separate netCDF files for each of the 16 spectral bands.
The L2 Cloud and Moisture Imagery (MCMIP) products contains all the 16 spectral bands into a single file.


## Cloud Bucket Products 
- AWS: - https://docs.opendata.aws/noaa-goes16/cics-readme.html
 

## Other python packages for GOES ABI download 
- [goes2go](https://github.com/blaylockbk/goes2go) (Note: has a nice documentation)
- [goesaws](https://github.com/mnichol3/goesaws) (Note: has a simple CLI)
- [goes-downloader](https://github.com/uba/goes-downloader) (Note: has a GUI)
- [goesmirror](https://github.com/meteoswiss-mdr/goesmirror/blob/master/goesmirror/goesmirror.py)
- [goes-py](https://github.com/palexandremello/goes-py)
- [GOES](https://github.com/joaohenry23/GOES)


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

Microsoft West Europe Azure Blob Storage  (only limited products)
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
- [adlfs](https://github.com/fsspec/adlfs)  (via azure.storage.blob ???)

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


##  THREDDS Catalogs
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


 
