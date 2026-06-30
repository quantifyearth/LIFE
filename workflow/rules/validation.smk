# LIFE Pipeline - Validation Rules
# =================================
#
# Validates AOH models for the "current" scenario only:
#
# 1. Model validation: Statistical analysis (requires R)
# 2. GBIF occurrence validation: Expensive, explicit-only

# =============================================================================
# Model Validation
# =============================================================================


rule model_validation:
    """
    Perform statistical validation of AOH models (Dahal et al. methodology).
    Runs on the "current" scenario AOHs only.
    """
    input:
        collated=DATADIR / "aohs" / "current.csv",
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        validation=DATADIR / "validation" / "model_validation.csv",
    log:
        DATADIR / "logs" / "model_validation.log",
    shell:
        """
        mkdir -p $(dirname {output.validation})
        aoh-validate-prevalence \
            --collated_aoh_data {input.collated} \
            --output {output.validation} \
            2>&1 | tee {log}
        """


# =============================================================================
# Species Richness and Endemism (current scenario)
# =============================================================================


rule species_richness:
    """
    Calculate species richness from current AOH rasters.
    NOT included in 'all' — use the 'summaries' target explicitly.
    """
    input:
        aoh_sentinel=DATADIR / "aohs" / "current.csv",
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        richness=DATADIR / "summaries" / "species_richness.tif",
    log:
        DATADIR / "logs" / "species_richness.log",
    threads: workflow.cores
    params:
        aohs_folder=DATADIR / "aohs" / "current",
    shell:
        """
        mkdir -p $(dirname {output.richness})
        aoh-species-richness \
            --aohs_folder {params.aohs_folder} \
            --output {output.richness} \
            2>&1 | tee {log}
        """


rule endemism:
    """
    Calculate endemism from current AOH rasters.
    NOT included in 'all' — use the 'summaries' target explicitly.
    """
    input:
        aoh_sentinel=DATADIR / "aohs" / "current.csv",
        species_richness=DATADIR / "summaries" / "species_richness.tif",
        version_sentinel=DATADIR / ".sentinels" / "aoh_version.txt",
    output:
        endemism=DATADIR / "summaries" / "endemism.tif",
    log:
        DATADIR / "logs" / "endemism.log",
    threads: workflow.cores
    params:
        aohs_folder=DATADIR / "aohs" / "current",
    shell:
        """
        aoh-endemism \
            --aohs_folder {params.aohs_folder} \
            --species_richness {input.species_richness} \
            --output {output.endemism} \
            2>&1 | tee {log}
        """


# =============================================================================
# Occurrence Validation (EXPENSIVE!)
# =============================================================================


rule fetch_gbif_data:
    """
    Fetch GBIF occurrence data for a taxa.
    Expensive (hours) — only runs if output doesn't exist.

    Environment variables required:
        GBIF_USERNAME, GBIF_EMAIL, GBIF_PASSWORD
    """
    input:
        collated=ancient(DATADIR / "aohs" / "current.csv"),
    output:
        sentinel=DATADIR / "validation" / "occurrences" / ".{taxa}_fetched",
    log:
        DATADIR / "logs" / "fetch_gbif_{taxa}.log",
    params:
        output_dir=DATADIR / "validation" / "occurrences",
    shell:
        """
        mkdir -p {params.output_dir}
        aoh-fetch-gbif-data \
            --collated_aoh_data {input.collated} \
            --taxa {wildcards.taxa} \
            --output_dir {params.output_dir} \
            2>&1 | tee {log}
        touch {output.sentinel}
        """


rule validate_gbif_occurrences:
    """
    Validate current AOH models against GBIF occurrence data.
    """
    input:
        gbif_sentinel=DATADIR / "validation" / "occurrences" / ".{taxa}_fetched",
        aoh_sentinel=ancient(DATADIR / "aohs" / "current" / "{taxa}" / ".complete"),
    output:
        validation=DATADIR / "validation" / "occurrences" / "{taxa}.csv",
    log:
        DATADIR / "logs" / "validate_gbif_{taxa}.log",
    params:
        gbif_data=lambda wildcards: DATADIR
        / "validation"
        / "occurrences"
        / wildcards.taxa,
        species_data=lambda wildcards: DATADIR
        / "species-info"
        / wildcards.taxa
        / "current",
        aoh_results=lambda wildcards: DATADIR / "aohs" / "current" / wildcards.taxa,
    shell:
        """
        aoh-validate-occurrences \
            --gbif_data_path {params.gbif_data} \
            --species_data {params.species_data} \
            --aoh_results {params.aoh_results} \
            --output {output.validation} \
            2>&1 | tee {log}
        """


rule occurrence_validation:
    """
    Target rule for GBIF validation for all taxa.
    WARNING: Expensive. Only run explicitly: snakemake occurrence_validation
    """
    input:
        expand(str(DATADIR / "validation" / "occurrences" / "{taxa}.csv"), taxa=TAXA),
