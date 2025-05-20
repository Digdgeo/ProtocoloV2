# Changelog

All notable changes to Protocolo v2 will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),  
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.2.3] - 2025-05-20
### Added
- Automatic generation of XML metadata for flood masks (ISO 19139)
- Export of coastal data including shoreline and fordune lines
- Export of flood summaries by polygon and lagoon to CSV
- Integration with GeoNetwork for metadata publication


### Changed
- Improved folder structure and product organization
- Logging system updated to be cleaner and more consistent
- MongoDB structure updated: `Flood` data grouped under `flood_data`
- All Landsat processing now goes through the `Product` class

### Fixed
- Bug when attaching quicklook in email report

