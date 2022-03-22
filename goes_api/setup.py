#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 22:16:25 2022

@author: ghiggi
"""
from pathlib import Path
from setuptools import setup, find_packages

HERE = Path(__file__).parent
README = (HERE / "README.md").read_text(encoding="utf8")

requires = ["numpy", "pandas", "trollsift", "fsspec", "s3fs", "gcsfs", "tqdm"]

setup(
    # Metadata
    name="goes_api",
    version="0.0.1",
    author="Gionata Ghiggi",
    author_email="gionata.ghiggi@gmail.com",
    description="Python Package for I/O of GOES-16/17 satellite data on local and cloud storage.",
    long_description=README,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 1 - Planning",  # https://pypi.org/classifiers/
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Natural Language :: English",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
    ],
    url="https://github.com/ghiggi/goes_api",
    project_urls={
        "Source Code": "https://github.com/ghiggi/goes_api",
        "Documentation": "https://ghiggi.github.io/goes_api/_build/html/",
    },
    license="GNU",
    # Options
    packages=find_packages(),
    package_data={
        "": ["*.cfg"],
    },
    keywords=[
        "GOES-16",
        "GOES-17",
        "ABI",
        "GLM",
        "satellite",
        "weather",
        "meteorology",
        "forecasting",
        "EWS",
    ],
    install_requires=requires,
    python_requires=">=3.7",
    zip_safe=False,
)

###############################################################################
## GOES2GO Note: How to upload a new version to PyPI
## -------------------------------------------------
# Created a new conda environment with twine
# conda create -n pypi python=3 twine pip -c conda-forge
"""
conda activate pypi
cd goes2go
python setup.py sdist bdist_wheel
twine check dist/*
# Test PyPI
twine upload --skip-existing --repository-url https://test.pypi.org/legacy/ dist/*
# PyPI
twine upload --skip-existing dist/*
"""
