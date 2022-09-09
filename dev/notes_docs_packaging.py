#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 22:09:56 2022

@author: ghiggi
"""
### Docs 
# https://github.com/pydata/pydata-sphinx-theme 


### Packaging
# - https://packaging.python.org/en/latest/
# - https://setuptools.pypa.io/en/latest/userguide/quickstart.html
# - https://pypi.org/project/twine/


# How to upload a new version to PyPI
## -------------------------------------------------
# Created a new conda environment with twine
# conda create -n pypi python=3 twine pip -c conda-forge
'''
conda activate pypi
cd goes_api
python setup.py sdist bdist_wheel
twine check dist/*
# Test PyPI
twine upload --skip-existing --repository-url https://test.pypi.org/legacy/ dist/*
# PyPI
twine upload --skip-existing dist/*
'''
 