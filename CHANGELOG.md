# Changelog

  All notable changes to this project will be documented in this file.

  The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
  and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

  ## [2.5.0] - 2025-11-14

  ### Added
  - Environment variables support for secure credential management
  - `.env.example` template file for easy configuration
  - `protocolo/config.py` centralized configuration module
  - `python-dotenv` dependency for loading environment variables

  ### Changed
  - Migrated hardcoded USGS credentials to environment variables
  - Migrated hardcoded PostgreSQL credentials to environment variables
  - Migrated hardcoded SSH configuration to environment variables
  - Migrated hardcoded server hosts to environment variables
  - Migrated hardcoded email recipients to environment variables
  - Updated `lagunas_labordette.csv` output to include only: `_id`, `NOMBRE`, `area_total`,
  `area_inundada`
  - Improved `.gitignore` to explicitly exclude `.env` files while allowing `.env.example`

  ### Security
  - Removed all hardcoded passwords and sensitive credentials from source code
  - Credentials now stored securely in `.env` file (not committed to repository)

  ### Removed
  - Shell script files (`.sh`) from protocolo directory
  - Old `codigo/` directory structure
  - Documentation source files from old structure

  ## [2.4.0] - Previous releases
  See git history for previous changes.

