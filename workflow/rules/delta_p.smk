# LIFE Pipeline - Delta P (Persistence) Rules
# ============================================
#
# Calculates per-species change in probability of persistence (Delta P)
# for each user-defined scenario, then aggregates to produce final maps.
#
# Pipeline per scenario:
# 1. calculate_delta_p: per species, uses current + scenario + pnv AOHs
# 2. aggregate_delta_p_per_taxa: sentinel that all species are done
# 3. raster_sum_per_taxa: sum per-species delta P values per taxa
# 4. species_totals: count species per taxa for normalisation
# 5. delta_p_scaled: final scaled output map


import os
from pathlib import Path


# =============================================================================
# Per-Species Delta P Calculation
# =============================================================================


rule calculate_delta_p:
    """
    Calculate the change in probability of persistence for a single species
    under a given scenario.

    Uses current, scenario, and PNV (historic) AOH rasters.
    RESIDENT species produce one TIF; NONBREEDING species produce one TIF
    (which covers the migratory pair); BREEDING species produce no output.

    A sentinel is always created to track completion regardless of season.
    """
    input:
        current_sentinel=DATADIR / "aohs" / "current" / "{taxa}" / ".complete",
        scenario_sentinel=DATADIR / "aohs" / "{scenario}" / "{taxa}" / ".complete",
        pnv_sentinel=DATADIR / "aohs" / "pnv" / "{taxa}" / ".complete",
    output:
        sentinel=DATADIR / "deltap" / "{scenario}" / CURVE / "{taxa}" / ".{species_id}.done",
    params:
        current_path=lambda wildcards: DATADIR / "aohs" / "current" / wildcards.taxa,
        scenario_path=lambda wildcards: DATADIR / "aohs" / wildcards.scenario / wildcards.taxa,
        pnv_path=lambda wildcards: DATADIR / "aohs" / "pnv" / wildcards.taxa,
        output_path=lambda wildcards: DATADIR / "deltap" / wildcards.scenario / CURVE / wildcards.taxa,
        curve=CURVE,
    log:
        DATADIR / "logs" / "deltap" / "{scenario}" / "{taxa}" / "{species_id}.log",
    shell:
        """
        mkdir -p $(dirname {log})
        mkdir -p {params.output_path}
        taxid=$(echo "{wildcards.species_id}" | cut -d_ -f1)
        season=$(echo "{wildcards.species_id}" | cut -d_ -f2)
        python3 {SRCDIR}/deltap/global_code_residents_pixel.py \
            --taxid "$taxid" \
            --season "$season" \
            --current_path {params.current_path} \
            --scenario_path {params.scenario_path} \
            --historic_path {params.pnv_path} \
            --output_path {params.output_path} \
            --z {params.curve} \
            2>&1 | tee {log}
        touch {output.sentinel}
        """


# =============================================================================
# Per-Taxa Delta P Aggregation
# =============================================================================


rule aggregate_delta_p_per_taxa:
    """
    Wait for all per-species delta P calculations for a taxa to complete.
    Creates a sentinel that downstream rules depend on.
    """
    input:
        sentinels=get_delta_p_sentinels_for_taxa_scenario,
    output:
        sentinel=DATADIR / "deltap" / "{scenario}" / CURVE / "{taxa}" / ".complete",
    shell:
        """
        touch {output.sentinel}
        """


# =============================================================================
# Per-Taxa Raster Sum
# =============================================================================


rule raster_sum_per_taxa:
    """
    Sum all per-species delta P rasters for a taxa into a single raster.
    """
    input:
        sentinel=DATADIR / "deltap" / "{scenario}" / CURVE / "{taxa}" / ".complete",
    output:
        DATADIR / "deltap_sum" / "{scenario}" / CURVE / "{taxa}.tif",
    params:
        rasters_dir=lambda wildcards: DATADIR / "deltap" / wildcards.scenario / CURVE / wildcards.taxa,
    threads: workflow.cores
    log:
        DATADIR / "logs" / "raster_sum" / "{scenario}" / "{taxa}.log",
    shell:
        """
        mkdir -p $(dirname {output})
        mkdir -p $(dirname {log})
        python3 {SRCDIR}/utils/raster_sum.py \
            --rasters_directory {params.rasters_dir} \
            --output {output} \
            2>&1 | tee {log}
        """


# =============================================================================
# Species Totals
# =============================================================================


rule species_totals:
    """
    Count the number of species per taxa used in the delta P calculation.
    Used by delta_p_scaled for normalisation.
    """
    input:
        sentinels=expand(
            str(DATADIR / "deltap" / "{{scenario}}" / CURVE / "{taxa}" / ".complete"),
            taxa=TAXA,
        ),
    output:
        totals=DATADIR / "deltap" / "{scenario}" / CURVE / "totals.csv",
    params:
        deltaps_dir=lambda wildcards: DATADIR / "deltap" / wildcards.scenario / CURVE,
    log:
        DATADIR / "logs" / "species_totals_{scenario}.log",
    shell:
        """
        python3 {SRCDIR}/utils/species_totals.py \
            --deltaps {params.deltaps_dir} \
            --output {output.totals} \
            2>&1 | tee {log}
        """


# =============================================================================
# Final Scaled Delta P Map
# =============================================================================


rule delta_p_scaled:
    """
    Generate the final scaled delta P map for a scenario.

    Combines per-taxa delta P sums with the habitat difference map and
    species totals to produce the final normalised LIFE output.
    """
    input:
        taxa_rasters=expand(
            str(DATADIR / "deltap_sum" / "{{scenario}}" / CURVE / "{taxa}.tif"),
            taxa=TAXA,
        ),
        diffmap=DATADIR / "habitat" / "{scenario}_diff_area.tif",
        totals=DATADIR / "deltap" / "{scenario}" / CURVE / "totals.csv",
    output:
        DATADIR / "deltap_final" / f"scaled_{{scenario}}_{CURVE}.tif",
    params:
        input_dir=lambda wildcards: DATADIR / "deltap_sum" / wildcards.scenario / CURVE,
    log:
        DATADIR / "logs" / "delta_p_scaled_{scenario}.log",
    shell:
        """
        mkdir -p $(dirname {output})
        python3 {SRCDIR}/deltap/delta_p_scaled.py \
            --input {params.input_dir} \
            --diffmap {input.diffmap} \
            --totals {input.totals} \
            --output {output} \
            2>&1 | tee {log}
        """
