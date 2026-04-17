# LIFE Pipeline - Species Data Extraction Rules
# ==============================================
#
# Extracts species data from the IUCN PostgreSQL database.
# Creates both current/ (presence codes 1,2) and historic/ (presence codes 1,2,4,5)
# subdirectories, each with per-species GeoJSON files and a report.csv.
#
# Environment variables required:
#   DB_HOST, DB_NAME, DB_USER, DB_PASSWORD


# =============================================================================
# Species Data Extraction (Checkpoint)
# =============================================================================


checkpoint extract_species_data:
    """
    Extract species data from PostgreSQL database.

    This is a checkpoint because the number of output GeoJSON files is only
    known after extraction. Each taxa produces N species files in both current/
    and historic/ subdirectories.

    Environment variables required:
        DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
    """
    output:
        current_report=DATADIR / "species-info" / "{taxa}" / "current" / "report.csv",
        historic_report=DATADIR / "species-info" / "{taxa}" / "historic" / "report.csv",
    params:
        classname="{taxa}",
        output_dir=lambda wildcards: DATADIR / "species-info" / wildcards.taxa,
        projection=config["projection"],
        overrides=lambda wildcards: DATADIR / config["optional_inputs"]["species_overrides"],
    resources:
        db_connections=1,
    threads: workflow.cores
    log:
        DATADIR / "logs" / "extract_species_{taxa}.log",
    shell:
        """
        OVERRIDES_ARG=""
        if [ -f "{params.overrides}" ]; then
            OVERRIDES_ARG="--overrides {params.overrides}"
        fi
        python3 {SRCDIR}/prepare_species/extract_species_psql.py \
            --class {wildcards.taxa} \
            --output {params.output_dir} \
            --projection "{params.projection}" \
            $OVERRIDES_ARG \
            2>&1 | tee {log}
        """
