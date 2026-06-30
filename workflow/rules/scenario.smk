# LIFE Pipeline - Scenario map rules
# =====================================
#
# Handles generating scenario-specific habitat maps and difference maps:
#
# - Arable: all non-urban land converted to arable
# - Restore variants: agricultural/pastoral land restored to PNV
#
# Each scenario:
# 1. Generate 100m scenario map from current map
# 2. Warp to habitat_layers/{scenario}/ at target pixel scale (PRECIOUS)
# 3. Generate diff map comparing current vs scenario


import os
from pathlib import Path

# IUCN codes to restore for each restore scenario variant
RESTORE_SCENARIOS = {
    "restore": "14.1,14.2,14.3,14.4,14.6",
    "restore_all": "14,14.1,14.2,14.3,14.4,14.5,14.6",
    "restore_agriculture": "14.1,14.2",
}

COUNTERFACTUAL_SCENARIOS = ["arable"] + list(RESTORE_SCENARIOS.keys())


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
    log:
        DATADIR / "logs" / "make_arable_map.log",
    threads: workflow.cores
    params:
        current_dir=DATADIR / "100m" / "current",
        output_dir=DATADIR / "100m" / "arable",
    shell:
        """
        python3 {SRCDIR}/prepare_layers/make_arable_map.py \
            --current {params.current_dir} \
            --output {params.output_dir} \
            -j {threads} \
            -p \
            2>&1 | tee {log}
        touch {output.sentinel}
        """


# =============================================================================
# Restore scenario variants
# =============================================================================


rule make_restore_scenario:
    """
    Generate a restore scenario map at 100m resolution.
    The IUCN habitat codes to restore are passed via --codes.
    """
    input:
        current_sentinel=DATADIR / "100m" / "current" / ".sentinel",
        pnv=DATADIR / "habitat" / "pnv_raw.tif",
        crosswalk=DATADIR / "crosswalk.csv",
    output:
        sentinel=DATADIR / "100m" / "{scenario}" / ".sentinel",
    log:
        DATADIR / "logs" / "make_{scenario}_map.log",
    wildcard_constraints:
        scenario="|".join(RESTORE_SCENARIOS.keys()),
    threads: workflow.cores
    params:
        current_dir=DATADIR / "100m" / "current",
        output_dir=lambda wc: DATADIR / "100m" / wc.scenario,
        codes=lambda wc: RESTORE_SCENARIOS[wc.scenario],
    shell:
        """
        python3 {SRCDIR}/prepare_layers/make_restore_map.py \
            --pnv {input.pnv} \
            --current {params.current_dir} \
            --crosswalk {input.crosswalk} \
            --codes {params.codes} \
            --output {params.output_dir} \
            -j {threads} \
            -p \
            2>&1 | tee {log}
        touch {output.sentinel}
        """


# =============================================================================
# Warp and diff rules (shared across all counterfactual scenarios)
# =============================================================================


rule warp_scenario:
    """
    Warp a scenario map from 100m to the target pixel scale.
    PRECIOUS: Only rebuilds if the sentinel is explicitly deleted.
    """
    input:
        sentinel=ancient(DATADIR / "100m" / "{scenario}" / ".sentinel"),
    output:
        sentinel=DATADIR / "habitat_layers" / "{scenario}" / ".sentinel",
    log:
        DATADIR / "logs" / "warp_{scenario}.log",
    wildcard_constraints:
        scenario="|".join(COUNTERFACTUAL_SCENARIOS),
    threads: workflow.cores
    params:
        input_dir=lambda wc: DATADIR / "100m" / wc.scenario,
        output_dir=lambda wc: DATADIR / "habitat_layers" / wc.scenario,
        pixel_scale=config["pixel_scale"],
    shell:
        """
        mkdir -p {params.output_dir}
        for d in {params.input_dir}/*.tif; do
            basename=$(basename "$d")
            gdalwarp \
                -t_srs EPSG:4326 \
                -tr {params.pixel_scale} -{params.pixel_scale} \
                -r average \
                -tap \
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


rule diff_map_scenario:
    """
    Generate the area difference map between current and a scenario's habitat layers.
    Used by the delta P scaling step.
    """
    input:
        current_sentinel=DATADIR / "habitat_layers" / "current" / ".sentinel",
        scenario_sentinel=DATADIR / "habitat_layers" / "{scenario}" / ".sentinel",
    output:
        DATADIR / "habitat" / "{scenario}_diff_area.tif",
    log:
        DATADIR / "logs" / "diff_map_{scenario}.log",
    wildcard_constraints:
        scenario="|".join(COUNTERFACTUAL_SCENARIOS),
    threads: workflow.cores
    params:
        current_dir=DATADIR / "habitat_layers" / "current",
        scenario_dir=lambda wc: DATADIR / "habitat_layers" / wc.scenario,
    shell:
        """
        mkdir -p $(dirname {output})
        python3 {SRCDIR}/prepare_layers/make_diff_map.py \
            --current {params.current_dir} \
            --scenario {params.scenario_dir} \
            --output {output} \
            -j {threads} \
            2>&1 | tee {log}
        """
