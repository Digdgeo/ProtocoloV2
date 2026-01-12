Protocolo Landsat v2 Documentation
===================================

Welcome to **Protocolo V2**, an automated processing pipeline for Landsat satellite imagery based on **Pseudo Invariant Areas (PIAs)** normalization.

Overview
--------

Protocolo V2 is designed to automate the complete workflow of Landsat image processing, from downloading scenes to generating derived environmental products. The system is optimized for continuous monitoring and runs automatically via cron jobs on a virtual machine.

Key Features
------------

* **Automated Landsat Processing**: Download and process Landsat Collection 2 Level-2 Surface Reflectance products (Landsat 5, 7, 8, and 9)
* **PIA-Based Normalization**: Normalize images using Pseudo Invariant Areas to ensure temporal consistency
* **Derived Product Generation**: Automatically generate NDVI, NDWI, MNDWI, flood masks, turbidity maps, and water depth calculations
* **Wetland Management**: Calculate hydroperiod values for environmental monitoring
* **Coastal Analysis**: Extract coastlines and analyze coastal dynamics
* **Database Integration**: Store metadata and results in MongoDB and PostgreSQL databases
* **Remote Distribution**: Automatically transfer results to remote servers for web visualization
* **Email Notifications**: Automated status reports and error notifications

Main Components
---------------

The system is built around several core modules:

**Landsat Class** (``protocolov2.py``)
   Handles all Landsat image processing operations including cloud masking, radiometric calibration,
   thermal correction, reprojection, and PIA-based normalization.

**Product Class** (``productos.py``)
   Generates derived environmental products from normalized Landsat scenes, including vegetation indices,
   water masks, inundation analysis, and turbidity calculations.

**Download Module** (``download.py``)
   Orchestrates the automated workflow by querying the USGS API for new Landsat scenes,
   managing downloads, and coordinating processing tasks.

**Coast Class** (``coast.py``)
   Specialized module for coastal analysis, including coastline extraction and sea level calculations.

**Hidroperiodo Module** (``hidroperiodo.py``)
   Calculates hydroperiod values for wetland management and environmental monitoring.

**Utilities** (``utils.py``)
   Collection of helper functions for email notifications, database operations, file transfers,
   and data visualization.

Workflow
--------

The typical processing workflow is:

1. **Scene Discovery**: Query USGS API for new Landsat scenes matching specified criteria (path/row, date range, cloud cover)
2. **Download**: Retrieve scene data and metadata from USGS
3. **Normalization**: Apply PIA-based normalization to ensure temporal consistency
4. **Product Generation**: Create derived products (NDVI, water masks, turbidity, etc.)
5. **Database Storage**: Store metadata and results in MongoDB
6. **Distribution**: Transfer results to remote servers via SSH
7. **Notification**: Send email reports with processing summary

Configuration
-------------

The system uses environment variables for all sensitive configuration (credentials, server addresses, etc.).
See ``.env.example`` for the complete list of required variables.

Main configuration parameters:

* USGS API credentials
* MongoDB connection settings
* PostgreSQL database configuration
* SSH server addresses and paths
* Email notification recipients
* GeoNetwork catalog credentials

Installation
------------

1. Clone the repository and navigate to the project directory
2. Create a conda environment from ``requirements.txt``
3. Copy ``.env.example`` to ``.env`` and configure your credentials
4. Configure cron jobs for automated execution

API Documentation
-----------------

.. toctree::
   :maxdepth: 3
   :caption: API Reference:

   modules

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
