# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
import logging

# Configurar logging para mostrar mensajes en consola
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Añadir la ruta de los módulos
sys.path.insert(0, os.path.abspath('../code'))

# Para verificar si la ruta es correcta
logger.info(f"Ruta agregada al PYTHONPATH: {os.path.abspath('../code')}")

project = 'ProtocoloV2'
copyright = '2024, Diego García Díaz'
author = 'Diego García Díaz'
release = '1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc',
    'sphinx.ext.napoleon']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
