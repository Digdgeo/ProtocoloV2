# Configuration file for the Sphinx documentation builder.

import os
import sys

# Añade la raíz del proyecto al sys.path (para que autodoc funcione)
codigo_path = os.path.abspath('../../')
print(">>> PATH DE CÓDIGO:", codigo_path)
sys.path.insert(0, codigo_path)

# -- Project information -----------------------------------------------------

project = 'Protocolo Landsat v2'
copyright = '2025, Diego García Díaz'
author = 'Diego García Díaz'
release = '2.5.0'

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',       # Para importar docstrings
    'sphinx.ext.napoleon',      # Para docstrings estilo Google/Numpy
    'sphinx.ext.viewcode',      # Añade enlaces al código fuente
    'myst_parser'               # Soporte para archivos Markdown (.md)
]

# Permite usar tanto .rst como .md
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

# (opcional, pero evita errores en versiones antiguas de Sphinx)
master_doc = 'index'

# Configuración de autodoc para definir el orden de los métodos (no alfabético)
autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'private-members': False,
    'special-members': '__init__',
    'show-inheritance': True,
    'exclude-members': '__weakref__',
}

autodoc_member_order = 'bysource'

# Simula módulos que no están disponibles en RTD
autodoc_mock_imports = [
    "osgeo",
    "rasterio",
    "rasterio.features",
    "rasterio.mask",
    "pymongo",
    "geopandas",
    "pyogrio",
    "fiona",
    "cv2",
    "scipy",
    "rasterstats",
    "xarray",
    "bottleneck",
    "gdal",
    "h5py",
    "netCDF4",
    "skimage",
    "psycopg2",
    "landsatxplore",
]

templates_path = ['_templates']
exclude_patterns = []

language = 'en'

# -- Options for HTML output -------------------------------------------------

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
