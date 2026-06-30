# LIFE implementation

This repository implements the LIFE extinction risk methodology as published in [Eyres et al](https://www.cambridge.org/engage/coe/article-details/66866978c9c6a5c07a3e07fa). The code will generate maps that cover the impact to extinction risk in an area under the two scenarios: conversion of the land to arable use, and coversion of the land to pre-human.

## Running the code

The methodology is explained in more detail in [method.md](method.md). The pipeline is implemented as a [Snakemake](https://snakemake.readthedocs.io/) workflow in `workflow/`.

### Prerequisites

The following tools must be available on `PATH`:

- `reclaimer` — downloads inputs from Zenodo ([quantifyearth/reclaimer](https://github.com/quantifyearth/reclaimer))
- `aoh-calc`, `aoh-habitat-process`, `aoh-collate-data`, `aoh-species-richness`, `aoh-endemism`, `aoh-validate-prevalence` — from the `aoh` Python package
- `gdalwarp` — part of GDAL

The following environment variables must be set:

| Variable | Purpose |
|----------|---------|
| `DATADIR` | Root directory for all input/output data |
| `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | PostgreSQL credentials for IUCN species database |

For GBIF occurrence validation (optional):

| Variable | Purpose |
|----------|---------|
| `GBIF_USERNAME`, `GBIF_EMAIL`, `GBIF_PASSWORD` | GBIF account credentials |

### Running the pipeline

```bash
# Full pipeline: delta P maps for all scenarios + model validation
snakemake --cores N all

# Delta P maps only, without validation
snakemake --cores N life

# Species richness and endemism maps (not included in 'all')
snakemake --cores N summaries

# Model validation only
snakemake --cores N validation

# GBIF occurrence validation (expensive — downloads gigabytes of data)
snakemake --cores N occurrence_validation
```

Other useful targets for incremental runs:

```bash
snakemake --cores N prepare       # Download and warp all base layers
snakemake --cores N species_data  # Extract species data from the database
snakemake --cores N aohs          # Generate all AOH rasters and collated CSVs
```

### Precious layers

The habitat and elevation layers are expensive to generate and are treated as **precious**: they will only be rebuilt if their output files are explicitly deleted, even if upstream inputs or code have changed. This applies to:

- `habitat_layers/current/` — food-enhanced current habitat map at ~1.67km resolution
- `habitat_layers/{scenario}/` — per-scenario fractional habitat maps
- `habitat_layers/pnv/` — potential natural vegetation fractional maps
- `elevation-max.tif`, `elevation-min.tif` — warped elevation layers

To force a rebuild of any of these, delete the relevant `.sentinel` file (or the output file itself) before re-running.

### Configuration

Pipeline parameters are in `config/config.yaml`:

- `taxa` — taxonomic classes to process (default: AMPHIBIA, AVES, MAMMALIA, REPTILIA)
- `scenarios` — habitat change scenarios to evaluate (default: arable, restore)
- `curve` — extinction curve exponent for delta P (default: `"0.25"`)
- `pixel_scale` — output raster resolution in degrees (default: ~5 arc-seconds)

### Inspecting the pipeline graph

The rule graph can be generated with:

```bash
snakemake --forceall --rulegraph 2>/dev/null | dot -Tsvg > graph.svg
```

Note that rules whose inputs are determined by the species-extraction checkpoint (`generate_aoh`, `calculate_delta_p`) will not appear in the static rule graph — this is a known snakemake limitation with checkpoint-based expansion. To see the complete job graph after species data has been extracted:

```bash
snakemake --forceall --dag 2>/dev/null | unflatten -f -l 5 | dot -Tsvg > dag.svg
```

## Credits

Originally derived from and using IUCN modlib and aoh lib by Daniele Baisero.
