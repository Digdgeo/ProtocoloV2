# Installation

This document explains how to install and run locally the protocol for downloading and processing Landsat Surface Reflectance (Level-2) images.

## 📦 Requirements

- Python >= 3.10, < 4.0 (recommended: 3.10.x)
- GDAL installed on the system (`libgdal >= 3.9.0`)
- Git
- `poetry` (dependency manager)
- Conda (optional but recommended to manage environments)

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

### 3. Install dependencies with Poetry

```bash
poetry install
```

> If you see errors related to `gdal`, make sure you have a compatible version of `libgdal` installed.  
> Check your version with:

```bash
gdalinfo --version
```

To install GDAL >= 3.9.0 on Ubuntu/Linux Mint:

```bash
sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
sudo apt update
sudo apt install -t ubuntugis-unstable gdal-bin libgdal-dev
```

### 4. Test the installation

Once everything is installed, run a quick test:

```python
from protocolo import Landsat
```

## 🧩 Package name vs. module name

- The installable package is called `protocolov2` (as defined in `pyproject.toml`)
- The actual Python module is in the `protocolo` folder

To import it in your code, use:

```python
from protocolo import Landsat
```

⚠️ Do not use `import protocolov2` — that is the name of the installable package, not the Python module.

## 🚀 Install as a package

To install the package from the project root:

```bash
pip install .
```

Or install directly from GitHub:

```bash
pip install git+https://github.com/Digdgeo/ProtocoloV2.git
```

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

- If using Read the Docs, make sure `.md` files are listed in `index.rst`
- The protocol is intended to run in a virtual machine with GDAL >= 3.9
- Tested successfully with Python 3.10.13 + GDAL 3.9.3