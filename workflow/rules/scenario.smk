# LIFE Pipeline - Scenario map rules
# =====================================
#
# Handles generating scenario-specific habitat maps and difference maps:
#
# - Arable: all non-urban land converted to arable
# - Restore: agricultural land restored to PNV
#
# Each scenario:
# 1. Generate 100m scenario map from current map
# 2. Warp to habitat_layers/{scenario}/ at target pixel scale (PRECIOUS)
# 3. Generate diff map comparing current vs scenario


import os
from pathlib import Path


# =============================================================================
# Arable scenario
# =============================================================================

rule make_arable_map:
    """
    Generate the arable scenario map at 100m resolution.
    All non-urban land is converted to arable.
    """
    input:
        current_sentinel=DATADIR / "100m" / "current" / ".sentinel",
    output:
        sentinel=DATADIR / "100m" / "arable" / ".sentinel",
    params:
        current_dir=DATADIR / "100m" / "current",
        output_dir=DATADIR / "100m" / "arable",
    threads: workflow.cores
    log:
        DATADIR / "logs" / "make_arable_map.log",
    shell:
        """
        python3 {SRCDIR}/prepare_layers/make_arable_map.py \
            --current {params.current_dir} \
            --output {params.output_dir} \
            -j 2 \
            2>&1 | tee {log}
        touch {output.sentinel}
        """


rule warp_arable:
    """
    Warp the arable scenario map from 100m to the target pixel scale.
    PRECIOUS: Only rebuilds if the sentinel is explicitly deleted.
    """
    input:
        sentinel=ancient(DATADIR / "100m" / "arable" / ".sentinel"),
    output:
        sentinel=DATADIR / "habitat_layers" / "arable" / ".sentinel",
    params:
        input_dir=DATADIR / "100m" / "arable",
        output_dir=DATADIR / "habitat_layers" / "arable",
        pixel_scale=config["pixel_scale"],
    threads: workflow.cores
    log:
        DATADIR / "logs" / "warp_arable.log",
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


rule diff_map_arable:
    """
    Generate the area difference map between current and arable habitat layers.
    Used by the delta P scaling step.
    """
    input:
        current_sentinel=DATADIR / "habitat_layers" / "current" / ".sentinel",
        arable_sentinel=DATADIR / "habitat_layers" / "arable" / ".sentinel",
    output:
        DATADIR / "habitat" / "arable_diff_area.tif",
    params:
        current_dir=DATADIR / "habitat_layers" / "current",
        arable_dir=DATADIR / "habitat_layers" / "arable",
    threads: workflow.cores
    log:
        DATADIR / "logs" / "diff_map_arable.log",
    shell:
        """
        mkdir -p $(dirname {output})
        python3 {SRCDIR}/prepare_layers/make_diff_map.py \
            --current {params.current_dir} \
            --scenario {params.arable_dir} \
            --output {output} \
            -j {threads} \
            2>&1 | tee {log}
        """


# =============================================================================
# Restore scenario
# =============================================================================

rule make_restore_map:
    """
    Generate the restore scenario map at 100m resolution.
    Agricultural land is restored to PNV habitat.
    """
    input:
        current_sentinel=DATADIR / "100m" / "current" / ".sentinel",
        pnv=DATADIR / "habitat" / "pnv_raw.tif",
        crosswalk=DATADIR / "crosswalk.csv",
    output:
        sentinel=DATADIR / "100m" / "restore" / ".sentinel",
    params:
        current_dir=DATADIR / "100m" / "current",
        output_dir=DATADIR / "100m" / "restore",
    threads: workflow.cores
    log:
        DATADIR / "logs" / "make_restore_map.log",
    shell:
        """
        python3 {SRCDIR}/prepare_layers/make_restore_map.py \
            --pnv {input.pnv} \
            --current {params.current_dir} \
            --crosswalk {input.crosswalk} \
            --output {params.output_dir} \
            -j 2 \
            2>&1 | tee {log}
        touch {output.sentinel}
        """


rule warp_restore:
    """
    Warp the restore scenario map from 100m to the target pixel scale.
    PRECIOUS: Only rebuilds if the sentinel is explicitly deleted.
    """
    input:
        sentinel=ancient(DATADIR / "100m" / "restore" / ".sentinel"),
    output:
        sentinel=DATADIR / "habitat_layers" / "restore" / ".sentinel",
    params:
        input_dir=DATADIR / "100m" / "restore",
        output_dir=DATADIR / "habitat_layers" / "restore",
        pixel_scale=config["pixel_scale"],
    threads: workflow.cores
    log:
        DATADIR / "logs" / "warp_restore.log",
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


rule diff_map_restore:
    """
    Generate the area difference map between current and restore habitat layers.
    """
    input:
        current_sentinel=DATADIR / "habitat_layers" / "current" / ".sentinel",
        restore_sentinel=DATADIR / "habitat_layers" / "restore" / ".sentinel",
    output:
        DATADIR / "habitat" / "restore_diff_area.tif",
    params:
        current_dir=DATADIR / "habitat_layers" / "current",
        restore_dir=DATADIR / "habitat_layers" / "restore",
    threads: workflow.cores
    log:
        DATADIR / "logs" / "diff_map_restore.log",
    shell:
        """
        mkdir -p $(dirname {output})
        python3 {SRCDIR}/prepare_layers/make_diff_map.py \
            --current {params.current_dir} \
            --scenario {params.restore_dir} \
            --output {output} \
            -j {threads} \
            2>&1 | tee {log}
        """
