# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

codigo_path = os.path.abspath('../../')
print(">>> PATH DE CÓDIGO:", codigo_path)
sys.path.insert(0, codigo_path)


project = 'Protocolo Landsat v2'
copyright = '2025, Diego García Díaz'
author = 'Diego García Díaz'
release = '2.2.3'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'myst_parser'
]

autodoc_mock_imports = [
    "osgeo",
    "rasterio",
    "pymongo",
    "geopandas",
    "pyogrio",
    "fiona",
    "cv2",
    "usgs",
    "scipy",
    "rasterstats",
    "xarray",
    "bottleneck",
    "gdal",
    "h5py",
    "netCDF4",
    "skimage",
    "cv2",
]

templates_path = ['_templates']
exclude_patterns = []

language = 'en'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
