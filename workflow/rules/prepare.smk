# LIFE Pipeline - Prepare base layers
# =====================================
#
# Handles:
# - Crosswalk table generation
# - Jung L2 habitat map download
# - Jung habitat updates download
# - Jung PNV map download
# - Elevation download and warp (precious)

import os
from pathlib import Path

# =============================================================================
# Crosswalk Table
# =============================================================================


rule convert_crosswalk:
    """
Generate a crosswalk between IUCN Habitat classes and Jung raster pixel values.
"""
    output:
        crosswalk=DATADIR / "crosswalk.csv",
    script:
        str(SRCDIR / "prepare_layers" / "generate_crosswalk.py")


# =============================================================================
# Jung L2 habitat map
# =============================================================================


rule jung_habitat_map:
    """
Fetch the Jung L2 habitat map from Zenodo.
"""
    output:
        habitat=DATADIR / "100m" / "jung_l2_raw.tif",
    log:
        DATADIR / "logs" / "download_habitat.log",
    params:
        zenodo_id=config["zenodo"]["jung_habitat"]["zenodo_id"],
        filename=config["zenodo"]["jung_habitat"]["filename"],
    shell:
        """
        mkdir -p $(dirname {output.habitat})
        reclaimer zenodo --zenodo_id {params.zenodo_id} \
                         --filename "{params.filename}" \
                         --extract \
                         --output {output.habitat} \
                         2>&1 | tee {log}
        """


rule jung_habitat_updates:
    """
Fetch the Jung habitat map update masks from Zenodo.
"""
    output:
        sentinel=DATADIR / "habitat" / ".downloaded_updates",
    log:
        DATADIR / "logs" / "download_habitat_updates.log",
    params:
        habitat_dir=DATADIR / "habitat",
        zenodo_id=config["zenodo"]["jung_habitat_updates"]["zenodo_id"],
        filename=config["zenodo"]["jung_habitat_updates"]["filename"],
    shell:
        """
        mkdir -p {params.habitat_dir}
        reclaimer zenodo --zenodo_id {params.zenodo_id} \
                        --filename "{params.filename}" \
                        --extract \
                        --output {params.habitat_dir} \
                        2>&1 | tee {log}
        touch {output.sentinel}
        """


# =============================================================================
# Jung PNV map
# =============================================================================


rule jung_pnv_map:
    """
Fetch the Jung PNV map from Zenodo.
"""
    output:
        habitat=DATADIR / "habitat" / "pnv_raw.tif",
    log:
        DATADIR / "logs" / "download_pnv.log",
    params:
        zenodo_id=config["zenodo"]["jung_pnv"]["zenodo_id"],
        filename=config["zenodo"]["jung_pnv"]["filename"],
    shell:
        """
        mkdir -p $(dirname {output.habitat})
        reclaimer zenodo --zenodo_id {params.zenodo_id} \
                         --filename "{params.filename}" \
                         --extract \
                         --output {output.habitat} \
                         2>&1 | tee {log}
        """


# =============================================================================
# Elevation layers (precious — only rebuild if explicitly deleted)
# =============================================================================


rule download_elevation:
    """
Download raw elevation DEM from Zenodo.
"""
    output:
        elevation=DATADIR / "elevation.tif",
    log:
        DATADIR / "logs" / "download_elevation.log",
    params:
        zenodo_id=config["zenodo"]["elevation"]["zenodo_id"],
        filename=config["zenodo"]["elevation"]["filename"],
    shell:
        """
        reclaimer zenodo --zenodo_id {params.zenodo_id} \
                         --filename "{params.filename}" \
                         --output {output.elevation} \
                         2>&1 | tee {log}
        """


rule elevation_max:
    """
Warp elevation to target projection and scale using max resampling.
Precious: only runs if output doesn't exist.
"""
    input:
        elevation=ancient(DATADIR / "elevation.tif"),
    output:
        elevation_max=DATADIR / "elevation-max.tif",
    log:
        DATADIR / "logs" / "elevation_max.log",
    threads: workflow.cores
    params:
        pixel_scale=config["pixel_scale"],
    shell:
        """
        gdalwarp \
            -t_srs EPSG:4326 \
            -tr {params.pixel_scale} -{params.pixel_scale} \
            -r max \
            -co COMPRESS=LZW \
            -wo NUM_THREADS={threads} \
            {input.elevation} \
            {output.elevation_max} \
            2>&1 | tee {log}
        """


rule elevation_min:
    """
Warp elevation to target projection and scale using min resampling.
Precious: only runs if output doesn't exist.
"""
    input:
        elevation=ancient(DATADIR / "elevation.tif"),
    output:
        elevation_min=DATADIR / "elevation-min.tif",
    log:
        DATADIR / "logs" / "elevation_min.log",
    threads: workflow.cores
    params:
        pixel_scale=config["pixel_scale"],
    shell:
        """
        gdalwarp \
            -t_srs EPSG:4326 \
            -tr {params.pixel_scale} -{params.pixel_scale} \
            -r min \
            -co COMPRESS=LZW \
            -wo NUM_THREADS={threads} \
            {input.elevation} \
            {output.elevation_min} \
            2>&1 | tee {log}
        """
