# Installation

This document explains how to install and run locally the protocol for downloading and processing Landsat Surface Reflectance (Level-2) images.

## 📦 Requirements

* Python >= 3.10, < 4.0 (recommended: 3.10.x)
* GDAL installed on the system (`libgdal >= 3.9.0`, tested with 3.9.3)
* Git
* `poetry` (dependency manager)
* Conda (optional but recommended to manage environments)

## 🛠️ Environment setup

### 1. Clone the repository

```bash
git clone https://github.com/Digdgeo/ProtocoloV2.git
cd ProtocoloV2
```

### 2. Create and activate your environment

If using Conda (recommended):

```bash
conda create -n Pv2 python=3.10
conda activate Pv2
```

Install `poetry` if you don't have it:

```bash
pip install poetry
```

### 3. Install GDAL using Conda

We **strongly recommend** installing GDAL using Conda *before* installing the Python package to avoid version conflicts:

```bash
conda install -c conda-forge gdal=3.9.3
```

Check your version with:

```bash
gdalinfo --version
```

> If you install GDAL using `apt` or other system package managers, make sure the version is >= 3.9 and compatible with your Python environment.

### 4. Install the Python package

If using Poetry:

```bash
poetry install
```

Or with pip (editable mode for development):

```bash
pip install -e .
```

Or directly from GitHub (last working version):

```bash
pip install git+https://github.com/Digdgeo/ProtocoloV2.git
```

### 5. Test the installation

Once everything is installed, run a quick test:

```python
from protocolo.protocolov2 import Landsat
```

## 🧹 Package name vs. module name

* The installable package is called `protocolov2` (as defined in `pyproject.toml`)
* The actual Python module is in the `protocolo` folder

To import it in your code, use:

```python
from protocolo.protocolov2 import Landsat
```

⚠️ Do not use import protocolov2 — that is the name of the installable package and also the name of the main Python file that contains the Landsat class (and will include the Sentinel2 class in the near future), not the name of the module to import directly.

## 🧪 For development without GDAL

If you only need to work on the documentation or code that doesn’t depend on GDAL, you can install without it:

```bash
poetry install --without gdal
```

> Useful in environments where `libgdal` is not available, or for quick editing.

## 🗂️ Project structure

```text
ProtocoloV2/
├── protocolo/           ← Main module with core classes and functions
├── docs/                ← Sphinx / Read the Docs documentation
├── tests/               ← (Optional) Unit tests
├── pyproject.toml       ← Poetry project configuration
├── README.md            ← Project overview
```

## 📚 Build the documentation locally

```bash
cd docs
make html
xdg-open build/html/index.html
```

## 🧵 Final notes

* If using Read the Docs, make sure `.md` files are listed in `index.rst`
* The protocol is intended to run in a virtual machine or Linux environment with GDAL >= 3.9
* Tested successfully with Python 3.10.13 + GDAL 3.9.3 + Conda + Poetry
