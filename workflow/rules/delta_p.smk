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
from types import SimpleNamespace

# =============================================================================
# Per-Species Delta P Calculation
# =============================================================================


rule calculate_delta_p:
    """
    Calculate the change in probability of persistence for a single species
    under a given scenario.

    species_id wildcard is of the form T{taxon_id}A{assessment_id}_{SEASON},
    e.g. T22685505A261477056_RESIDENT.
    """
    input:
        current_sentinel=DATADIR / "aohs" / "current" / "{taxa}" / ".complete",
        scenario_sentinel=DATADIR / "aohs" / "{scenario}" / "{taxa}" / ".complete",
        pnv_sentinel=DATADIR / "aohs" / "pnv" / "{taxa}" / ".complete",
    output:
        sentinel=DATADIR
        / "deltap"
        / "{scenario}"
        / CURVE
        / "{taxa}"
        / ".{species_id}.done",
    log:
        DATADIR / "logs" / "deltap" / "{scenario}" / "{taxa}" / "{species_id}.log",
    params:
        current_path=lambda wildcards: DATADIR / "aohs" / "current" / wildcards.taxa,
        scenario_path=lambda wildcards: DATADIR
        / "aohs"
        / wildcards.scenario
        / wildcards.taxa,
        pnv_path=lambda wildcards: DATADIR / "aohs" / "pnv" / wildcards.taxa,
        taxon_id=lambda wildcards: wildcards.species_id.rsplit("_", 1)[0],
        season=lambda wildcards: wildcards.species_id.rsplit("_", 1)[1],
        curve=CURVE,
        output_tif=lambda wildcards: DATADIR
        / "deltap"
        / wildcards.scenario
        / CURVE
        / wildcards.taxa
        / f"deltap_{wildcards.species_id}.tif",
    script:
        str(SRCDIR / "deltap" / "global_code_residents_pixel.py")


# =============================================================================
# Per-Taxa Raster Sum
# =============================================================================


rule raster_sum_per_taxa:
    """
    Sum all per-species delta P rasters for a taxa into a single raster.
    Implicitly waits for all calculate_delta_p jobs via direct tif dependencies.
    """
    input:
        rasters=get_delta_p_sentinels_for_taxa_scenario,
    output:
        tif=DATADIR / "deltap_sum" / "{scenario}" / CURVE / "{taxa}.tif",
    log:
        DATADIR / "logs" / "raster_sum" / "{scenario}" / "{taxa}.log",
    threads: workflow.cores
    params:
        rasters_dir=lambda wildcards: DATADIR
        / "deltap"
        / wildcards.scenario
        / CURVE
        / wildcards.taxa,
        curve=CURVE,
    script:
        str(SRCDIR / "utils" / "raster_sum.py")


# =============================================================================
# Species Totals
# =============================================================================


def get_all_delta_p_tifs_for_scenario(wildcards):
    tifs = []
    for taxa in TAXA:
        mock = SimpleNamespace(taxa=taxa, scenario=wildcards.scenario)
        tifs.extend(get_delta_p_sentinels_for_taxa_scenario(mock))
    return tifs


rule species_totals:
    """
    Count the number of species per taxa used in the delta P calculation.
    Used by delta_p_scaled for normalisation.
    """
    input:
        rasters=get_all_delta_p_tifs_for_scenario,
    output:
        totals=DATADIR / "deltap" / "{scenario}" / CURVE / "totals.csv",
    log:
        DATADIR / "logs" / "species_totals_{scenario}.log",
    params:
        deltaps_dir=lambda wildcards: DATADIR / "deltap" / wildcards.scenario / CURVE,
    script:
        str(SRCDIR / "utils" / "species_totals.py")


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
        final=DATADIR / "deltap_final" / f"scaled_{{scenario}}_{CURVE}.tif",
    log:
        DATADIR / "logs" / "delta_p_scaled_{scenario}.log",
    params:
        input_dir=lambda wildcards: DATADIR / "deltap_sum" / wildcards.scenario / CURVE,
    script:
        str(SRCDIR / "deltap" / "delta_p_scaled.py")
