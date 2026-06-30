# LIFE Pipeline - Area of Habitat (AoH) Generation Rules
# =======================================================
#
# Generates AoH rasters for each species across all scenarios.
# Scenarios: current, pnv, and user-defined scenarios (arable, restore, ...)
#
# For current/scenario AOHs: uses species-info/{taxa}/current/ geojsons
# For pnv AOHs: uses species-info/{taxa}/historic/ geojsons
#
# Code-sensitive: rebuilds if the aoh package version changes.

import os
from pathlib import Path

# =============================================================================
# Version Sentinel for AOH Code
# =============================================================================


rule aoh_version_sentinel:
    """
    Track the aoh package version. AOH rules depend on this to trigger
    rebuilds when the package updates.
    """
    output:
        sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    run:
        import subprocess

        os.makedirs(os.path.dirname(output.sentinel), exist_ok=True)
        try:
            result = subprocess.run(
                ["aoh-calc", "--version"],
                capture_output=True,
                text=True,
                check=True,
            )
            aoh_version = result.stdout.strip()
        except Exception:
            aoh_version = "unknown"
        with open(output.sentinel, "w") as f:
            f.write(f"aoh: {aoh_version}\n")


# =============================================================================
# Per-Species AOH Generation
# =============================================================================


def aoh_species_inputs(wildcards):
    """Return inputs for generate_aoh."""
    era = "historic" if wildcards.scenario == "pnv" else "current"
    return {
        "species_data": DATADIR
        / "species-info"
        / wildcards.taxa
        / era
        / f"range_{wildcards.species_id}.geojson",
        "habitat_sentinel": ancient(
            DATADIR / "habitat_layers" / wildcards.scenario / ".sentinel"
        ),
        "elevation_max": ancient(DATADIR / "elevation-max.tif"),
        "elevation_min": ancient(DATADIR / "elevation-min.tif"),
        "crosswalk": DATADIR / "crosswalk.csv",
        "version_sentinel": DATADIR / ".sentinels" / "aoh_version.txt",
    }


rule generate_aoh:
    """
    Generate Area of Habitat raster for a single species in a given scenario.

    Parallelizable: run with `snakemake --cores N` to process multiple species
    concurrently. Uses --force-habitat and --pixel-area flags (no mask).

    For current/scenario AOHs: uses current/ era species data.
    For pnv AOHs: uses historic/ era species data.
    """
    input:
        unpack(aoh_species_inputs),
    output:
        metadata=DATADIR / "aohs" / "{scenario}" / "{taxa}" / "aoh_{species_id}.json",
    log:
        DATADIR / "logs" / "aoh" / "{scenario}" / "{taxa}" / "aoh_{species_id}.log",
    resources:
        aoh_slots=1,
    params:
        habitat_dir=lambda wildcards: DATADIR / "habitat_layers" / wildcards.scenario,
        output_dir=lambda wildcards: DATADIR
        / "aohs"
        / wildcards.scenario
        / wildcards.taxa,
    shell:
        """
        mkdir -p $(dirname {log})
        mkdir -p {params.output_dir}
        aoh-calc \
            --fractional_habitats {params.habitat_dir} \
            --elevation-max {input.elevation_max} \
            --elevation-min {input.elevation_min} \
            --crosswalk {input.crosswalk} \
            --speciesdata {input.species_data} \
            --output {params.output_dir} \
            --force-habitat \
            --pixel-area \
            2>&1 | tee {log}
        """


# =============================================================================
# Per-Taxa AOH Aggregation (Checkpoint)
# =============================================================================


rule aggregate_aohs_per_taxa:
    """
    Checkpoint that ensures all AOHs for a taxa/scenario are generated.
    Creates a sentinel file when complete.

    This is a checkpoint so downstream rules (delta P) can re-evaluate the DAG
    after AOHs are created.
    """
    input:
        metadata=get_all_aoh_metadata_for_taxa_scenario,
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        sentinel=DATADIR / "aohs" / "{scenario}" / "{taxa}" / ".complete",
    shell:
        """
        echo "Generated $(echo {input.metadata} | wc -w) AOHs for {wildcards.taxa}/{wildcards.scenario}"
        touch {output.sentinel}
        """


# =============================================================================
# Collate AOH Data (per scenario)
# =============================================================================


rule collate_aoh_data:
    """
    Collate metadata from all AOH JSON files for a scenario into a single CSV.

    Used by validation (current scenario) and for downstream analysis.
    """
    input:
        sentinels=lambda wildcards: expand(
            str(DATADIR / "aohs" / "{scenario}" / "{taxa}" / ".complete"),
            scenario=wildcards.scenario,
            taxa=TAXA,
        ),
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        collated=DATADIR / "aohs" / "{scenario}.csv",
    log:
        DATADIR / "logs" / "collate_aoh_{scenario}.log",
    params:
        aoh_results_dir=lambda wildcards: DATADIR / "aohs" / wildcards.scenario,
    shell:
        """
        mkdir -p $(dirname {output.collated})
        aoh-collate-data \
            --aoh_results {params.aoh_results_dir} \
            --output {output.collated} \
            2>&1 | tee {log}
        """


# =============================================================================
# Footprint of Humanity (per scenario)
# =============================================================================


rule footprint_of_humanity:
    """
    Compute the footprint of humanity metric for a given scenario.

    Compares current AOH data against PNV and the scenario CSVs.
    """
    input:
        pnv_csv=DATADIR / "aohs" / "pnv.csv",
        scenario_csv=DATADIR / "aohs" / "{scenario}.csv",
        current_csv=DATADIR / "aohs" / "current.csv",
    output:
        DATADIR / "footprint" / "{scenario}.csv",
    log:
        DATADIR / "logs" / "footprint_{scenario}.log",
    wildcard_constraints:
        scenario="|".join(COUNTERFACTUAL_SCENARIOS),
    shell:
        """
        mkdir -p $(dirname {output})
        python3 {SRCDIR}/utils/footprint_of_humanity.py \
            --current {input.current_csv} \
            --pnv {input.pnv_csv} \
            --scenario {input.scenario_csv} \
            --output {output} \
            2>&1 | tee {log}
        """
