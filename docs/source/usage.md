# Usage of the Landsat Protocol

This page describes the folder structure and general workflow of the Landsat processing protocol.

## Folder Structure

The base directory for each project is expected to follow this structure:

```
.
├── ori/        # Original scenes downloaded from USGS (e.g. .tar files)
├── rad/        # Radiometrically corrected scenes
├── geo/        # Georeferenced scenes
├── nor/        # Normalized scenes (analysis-ready)
├── pro/        # Generated products (masks, CSV, quicklooks)
│   └── {scene_id}/
│       ├── quicklook.png
│       ├── SUPERFICIE_INUNDADA.csv
│       └── ...
├── temp/       # Temporary working files during processing
├── data/       # External shapefiles and auxiliary data
│   ├── recintos.shp
│   ├── lagunas.shp
│   ├── costa_extent.shp
├── hyd/        # Hydroperiod time series outputs (per year/cycle)
```

> 📝 A `logs/` folder is not currently implemented, but could be added for tracking the processing status of each scene.

---

## Workflow Overview

The protocol executes the following main steps:

1. **Download and Extraction**
   Scenes are retrieved from the USGS API and stored under `ori/rar`.

2. **Scene Initialization (`Landsat`)**

   * Parses the MTL file for metadata.
   * Generates the quicklook image.
   * Inserts the scene's metadata into the MongoDB database (`Landsat` collection).

3. **Main Processing (`Product`)**

   * Computes cloud and water masks.
   * Extracts inundated area per polygon (e.g., marshland units).
   * Calculates lagoon statistics and links them to the scene.
   * Exports `.csv` and `.png` products.

4. **Optional: Coastline Analysis (`Coast`)**

   * Extracts coastline from the water mask.
   * Links the coastline to tide level at acquisition time.
   * Stores results in the `pro/{scene_id}/` folder.

5. **Optional: Hydroperiods**

   * Outputs multi-year summaries per pixel in the `hyd/` folder, grouped by hydrological cycle.

---

## Notes

* MongoDB is used to store structured metadata for each scene.
* Products can optionally be exported or synchronized to an external NAS or PostgreSQL.
* The protocol is modular and can be run step-by-step or integrated into a cron job.
