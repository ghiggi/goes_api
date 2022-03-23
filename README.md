# GOES API - An API to download and query GOES third generation satellite data.

The code in this repository provides an API to donwload, query and filter GOES16 and GOES17 satellite data.

Documentation is available at XXXXX

ATTENTION: The code will like be the subject of progressive updates in the coming  months.

The folder `docs` contains documents with various information related to GOES data products.
The folder `tutorials` provide jupyter notebooks describing various features of GOES_API.

- Downloading GOES data: [`download.ipynb`]
- Find and filter GOES data: [`find_and_filter.ipynb`]
- Read data directly from AWS S3 and GCS buckets: [`read_bucket_data.ipynb`]
- Read ABI L1B and L2B products from cloud buckets and plot it with satpy: [`read_bucket_data_with_satpy.ipynb`]
- Extract zarr reference files using kerchunk: [`kerchunk_data.ipynb`]

[`download.ipynb`]: https://github.com/ghiggi/goes_api/blob/main/tutorials/00_download_and_find_files.py
[`find_and_filter.ipynb`]: https://github.com/ghiggi/goes_api/blob/main/tutorials/01_find_utility.py
[`read_bucket_data.ipynb`]: https://github.com/ghiggi/goes_api/blob/main/tutorials/03_read_cloud_bucket_data.py
[`read_bucket_data_with_satpy.ipynb`]: https://github.com/ghiggi/goes_api/blob/main/tutorials/03_read_cloud_bucket_data_with_satpy.py
[`kerchunk_data.ipynb`]: https://github.com/ghiggi/goes_api/blob/main/tutorials/04_kerchunk_dataset.py

## Installation

For a local installation, follow the below instructions.

1. Clone this repository.
   ```sh
   git clone git@github.com:ghiggi/goes_api.git
   cd goes_api
   ```

2. Install the dependencies using conda:
   ```sh
   conda env create -f environment.yml
   ```
   
3. Activate the goes_api conda environment 
   ```sh
   conda activate goes_api
   ```

4. Alternatively install manually the required packages with 
   ```sh
   conda install -c conda-forge dask numpy pandas trollsift fsspec s3fs gcsfs kerchunk ujson tqdm
   ```
 
## Contributors

* [Gionata Ghiggi](https://people.epfl.ch/gionata.ghiggi)

## License

The content of this repository is released under the terms of the [GNU General Public License v3.0](LICENSE.txt).
