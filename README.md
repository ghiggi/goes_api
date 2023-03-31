# GOES API - An API to download and query GOES satellite data.
[![DOI](https://zenodo.org/badge/380300525.svg)](https://zenodo.org/badge/latestdoi/380300525)
[![PyPI version](https://badge.fury.io/py/goes_api.svg)](https://badge.fury.io/py/goes_api)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/goes_api.svg)](https://anaconda.org/conda-forge/goes_api)
[![Build Status](https://github.com/ghiggi/goes_api/workflows/Continuous%20Integration/badge.svg?branch=main)](https://github.com/ghiggi/goes_api/actions)
[![Coverage Status](https://coveralls.io/repos/github/ghiggi/goes_api/badge.svg?branch=main)](https://coveralls.io/github/ghiggi/goes_api?branch=main)
[![Documentation Status](https://readthedocs.org/projects/goes_api/badge/?version=latest)](https://gpm_api.readthedocs.io/projects/goes_api/en/stable/?badge=stable)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/github/license/ghiggi/goes_api)](https://github.com/ghiggi/goes_api/blob/master/LICENSE)

The code in this repository provides an API to download, query and filter GOES16 and GOES17 satellite data.

Data download and query/filtering is available:
- for local file systems, Google Cloud Storage and AWS S3.
- for sensors ABI, EXIS, GLM, MAG, SEIS and SUVI.

The folder `tutorials` provide the following jupyter notebooks, describing various features of `GOES_API`:

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

The folder `docs` contains documents with various information related to GOES data products.

Documentation is available at XXXXX

## Installation

### pip

GOES-API can be installed via [pip][pip_link] on Linux, Mac, and Windows.
On Windows you can install [WinPython][winpy_link] to get Python and pip running.

Then, install the GOES-API package by typing the following command in the command terminal:

    pip install goes_api

## Contributors

* [Gionata Ghiggi](https://people.epfl.ch/gionata.ghiggi)

## License

The content of this repository is released under the terms of the [GNU General Public License v3.0](LICENSE.txt).
