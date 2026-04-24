# LIFE Pipeline - Habitat layer rules
# =====================================
#
# Handles the two-phase habitat map generation:
#
# Phase 1 (generate_food_map.sh equivalent):
#   - Download GAEZ and HYDE data
#   - Combine GAEZ/HYDE into crop/pasture fractional layers
#   - Build Jung current map at 100m
#   - Rescale PNV to 100m
#   - Build food-enhanced current map at 100m (PRECIOUS)
#
# Phase 2 (run.sh equivalent):
#   - Warp current 100m → habitat_layers/current/ (PRECIOUS)
#   - Process PNV → habitat_layers/pnv/ via aoh-habitat-process (PRECIOUS)


import os
from pathlib import Path


# =============================================================================
# GAEZ data
# =============================================================================

rule gaez_download:
    """
    Fetch the compressed GAEZ data.
    """
    output:
        archive=DATADIR / "food" / "LR.zip"
    params:
        url="https://s3.eu-west-1.amazonaws.com/data.gaezdev.aws.fao.org/LR.zip",
    log:
        DATADIR / "logs" / "download_gaez.log",
    shell:
        """
        mkdir -p $(dirname {output.archive})
        curl -o {output.archive} \
            {params.url} \
            2>&1 | tee {log}
        """


rule gaez_expand:
    """
    Decompress the GAEZ download.
    """
    output:
        raster=DATADIR / "food" / "GLCSv11_02_5m.tif"
    params:
        filepath="LR/lco/GLCSv11_02_5m.tif"
    input:
        archive=DATADIR / "food" / "LR.zip"
    log:
        DATADIR / "logs" / "expand_gaez.log",
    shell:
        """
        unzip -j {input.archive} \
            {params.filepath} \
            -d $(dirname {output.raster}) \
            2>&1 | tee {log}
        """

# =============================================================================
# Hyde data
# =============================================================================

rule hyde_download:
    """Fetch the compressed HYDE data."""
    output:
        archive=DATADIR / "food" / "baseline.zip"
    params:
        url="https://geo.public.data.uu.nl/vault-hyde/HYDE%203.2%5B1710494848%5D/original/baseline.zip",
    log:
        DATADIR / "logs" / "download_hyde.log",
    shell:
        """
        mkdir -p $(dirname {output.archive})
        curl -o {output.archive} \
            {params.url} \
            2>&1 | tee {log}
        """


rule hyde_expand_land_usage_archive:
    """Extract the inner land usage archive from HYDE."""
    input:
        archive=DATADIR / "food" / "baseline.zip"
    output:
        inner_hyde=DATADIR / "food" / "2017AD_lu.zip"
    params:
        filepath="baseline/zip/2017AD_lu.zip"
    log:
        DATADIR / "logs" / "expand_hyde_1.log",
    shell:
        """
        unzip -j {input.archive} \
            {params.filepath} \
            -d $(dirname {output.inner_hyde}) \
            2>&1 | tee {log}
        """


rule hyde_expand_land_usage_raster:
    """Extract raster from the inner HYDE land usage archive."""
    input:
        archive=DATADIR / "food" / "2017AD_lu.zip"
    output:
        raw_hyde_raster=DATADIR / "food" / "grazing2017AD.asc"
    params:
        filepath="grazing2017AD.asc"
    log:
        DATADIR / "logs" / "expand_hyde_2.log",
    shell:
        """
        unzip -j {input.archive} \
            {params.filepath} \
            -d $(dirname {output.raw_hyde_raster}) \
            2>&1 | tee {log}
        """


rule modify_hyde_pixel_scale:
    """
    Fix the pixel scale value in the HYDE data — the rounding is not precise
    enough to align with the GAEZ data.
    """
    input:
        raw_hyde_raster=DATADIR / "food" / "grazing2017AD.asc"
    output:
        modified_hyde_raster=DATADIR / "food" / "modified_grazing2017AD.asc"
    shell:
        """
        sed "s/0.0833333/0.08333333333333333/" \
            {input.raw_hyde_raster} > {output.modified_hyde_raster}
        """


rule add_hyde_projection_info:
    """
    The HYDE data ships without a projection, so add one so that
    the rest of the workflow can mix it with projected raster data.
    """
    output:
        hyde_projection_file=DATADIR / "food" / "modified_grazing2017AD.prj"
    params:
        projection=config["hyde_projection"]
    shell:
        """
        echo '{params.projection}' > {output.hyde_projection_file}
        """


# =============================================================================
# Combine GAEZ/Hyde data
# =============================================================================
rule combine_gaez_hyde:
    """
    Combine the GAEZ and Hyde data, adjusting for overflow in cells.
    """
    input:
        hyde_projection_file=DATADIR / "food" / "modified_grazing2017AD.prj",
        hyde_raster=DATADIR / "food" / "modified_grazing2017AD.asc",
        gaez_raster=DATADIR / "food" / "GLCSv11_02_5m.tif",
    output:
        crop=DATADIR / "food" / "crop.tif",
        pasture=DATADIR / "food" / "pasture.tif"
    params:
        output_dir=DATADIR / "food"
    script:
        str(SRCDIR / "prepare_layers" / "build_gaez_hyde.py")


# =============================================================================
# PNV rescaled to 100m
# =============================================================================

rule pnv_100m:
    """
    Rescale the PNV map to 100m resolution.
    Yirgacheffe can rescale dynamically, but pre-scaling is faster at the cost
    of some extra disk space.
    """
    input:
        pnv=DATADIR / "habitat" / "pnv_raw.tif",
    output:
        pnv_100m=DATADIR / "100m" / "pnv.tif",
    log:
        DATADIR / "logs" / "pnv_100m.log",
    shell:
        """
        gdalwarp \
            -t_srs EPSG:4326 \
            -tr 0.000898315284120 -0.000898315284120 \
            -r near \
            -tap \
            -multi -wo NUM_THREADS=ALL_CPUS \
            -co COMPRESS=LZW -co NUM_THREADS=ALL_CPUS \
            {input.pnv} \
            {output.pnv_100m} \
            2>&1 | tee {log}
        """


# =============================================================================
# Food-enhanced current map at 100m (PRECIOUS)
# =============================================================================

rule current_raws:
    """
    Build the LIFE current map, which is Jung with updates applied
    and restricted to L1 to match the PNV map restrictions.
    """
    input:
        updates_sentinel=DATADIR / "habitat" / ".downloaded_updates",
        habitat=DATADIR / "100m" / "jung_l2_raw.tif",
        crosswalk=DATADIR / "crosswalk.csv",
    params:
        updates_dir=DATADIR / "habitat" / "lvl2_changemasks_ver004",
        output_dir=DATADIR / "100m" / "jung_current",
    output:
        sentinel=DATADIR / "100m" / "jung_current" / ".sentinel",
    threads: workflow.cores
    script:
        str(SRCDIR / "prepare_layers" / "make_current_map.py")

rule build_food_map:
    """
    Build the food-enhanced current habitat map at 100m resolution by combining
    the Jung current map with GAEZ/HYDE crop and pasture fractions.

    PRECIOUS: Only rebuilds if the sentinel is explicitly deleted.
    This is the most expensive step in the pipeline.
    """
    input:
        jung=ancient(DATADIR / "100m" / "jung_current" / ".sentinel"),
        pnv=ancient(DATADIR / "100m" / "pnv.tif"),
        crop=ancient(DATADIR / "food" / "crop.tif"),
        pasture=ancient(DATADIR / "food" / "pasture.tif"),
    output:
        sentinel=DATADIR / "100m" / "current" / ".sentinel",
    params:
        jung_dir=DATADIR / "100m" / "jung_current",
        output_dir=DATADIR / "100m" / "current",
    threads: workflow.cores
    log:
        DATADIR / "logs" / "build_food_map.log",
    script:
        str(SRCDIR / "prepare_layers" / "make_food_current_map.py")


# =============================================================================
# Warp current map to habitat_layers resolution (PRECIOUS)
# =============================================================================

rule warp_current:
    """
    Warp the food-enhanced current map from 100m to the target pixel scale
    (5 arc-seconds, ~1.67km at the equator).

    PRECIOUS: Only rebuilds if the sentinel is explicitly deleted.
    """
    input:
        sentinel=ancient(DATADIR / "100m" / "current" / ".sentinel"),
    output:
        sentinel=DATADIR / "habitat_layers" / "current" / ".sentinel",
    params:
        input_dir=DATADIR / "100m" / "current",
        output_dir=DATADIR / "habitat_layers" / "current",
        pixel_scale=config["pixel_scale"],
    threads: workflow.cores
    log:
        DATADIR / "logs" / "warp_current.log",
    shell:
        """
        mkdir -p {params.output_dir}
        for d in {params.input_dir}/*.tif; do
            basename=$(basename "$d")
            gdalwarp \
                -t_srs EPSG:4326 \
                -tr {params.pixel_scale} -{params.pixel_scale} \
                -r average \
                -multi \
                -co COMPRESS=LZW \
                -co NUM_THREADS={threads} \
                -wo NUM_THREADS={threads} \
                "$d" \
                {params.output_dir}/"$basename" \
                2>&1
        done 2>&1 | tee {log}
        touch {output.sentinel}
        """


# =============================================================================
# Process PNV map to habitat_layers (PRECIOUS)
# =============================================================================

rule pnv_processed:
    """
    Process the PNV map into per-class fractional rasters at the target pixel scale
    using aoh-habitat-process.

    PRECIOUS: Only rebuilds if the sentinel is explicitly deleted.
    """
    input:
        pnv=ancient(DATADIR / "habitat" / "pnv_raw.tif"),
    output:
        sentinel=DATADIR / "habitat_layers" / "pnv" / ".sentinel",
    params:
        output_dir=DATADIR / "habitat_layers" / "pnv",
        pixel_scale=config["pixel_scale"],
        projection=config["projection"],
    log:
        DATADIR / "logs" / "pnv_processed.log",
    shell:
        """
        set -e
        aoh-habitat-process \
            --habitat {input.pnv} \
            --scale {params.pixel_scale} \
            --projection {params.projection} \
            --output {params.output_dir} \
            2>&1 | tee {log}
        touch {output.sentinel}
        """
