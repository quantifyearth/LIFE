# LIFE Pipeline - Prepare base layers
# =====================================


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
    Fetch the Jung habitat map used as the current map.
    """
    output:
        habitat=DATADIR / "habitat" / "jung_l2_raw.tif"
    params:
        zenodo_id=config["zenodo"]["jung_habitat"]["zenodo_id"],
        filename=config["zenodo"]["jung_habitat"]["filename"],
    log:
        DATADIR / "logs" / "download_habitat.log",
    shell:
        """
        reclaimer zenodo --zenodo_id {params.zenodo_id} \
                         --filename "{params.filename}" \
                         --extract \
                         --output {output.habitat} \
                         2>&1 | tee {log}
        """

rule jung_habitat_updates:
    """
    Fetch the Jung habitat map updates.
    """
    output:
        sentinel=DATADIR / "habitat" / ".downloaded_updates"
    params:
        habitat_dir=DATADIR / "habitat",
        zenodo_id=config["zenodo"]["jung_habitat"]["zenodo_id"],
        filename=config["zenodo"]["jung_habitat"]["filename"],
    log:
        DATADIR / "logs" / "download_habitat_update.log",
    shell:
        """
        reclaimer zenodo --zenodo_id {params.zenodo_id} \
                        --filename "{params.filename}" \
                        --extract \
                        --output {params.habitat_dir} \
                        2>&1 | tee {log}

        touch {output.sentinel}
        """

rule current_raws:
    """
    Build the LIFE current map, which is Jung with updates applied
    and restricted to L1 to match the PNV map restrictions.
    """
    input:
        updates_sentinel=DATADIR / "habitat" / ".downloaded_updates",
        habitat=DATADIR / "habitat" / "jung_l2_raw.tif",
        crosswalk=DATADIR / "crosswalk.csv",
    params:
        updates_dir=DATADIR / "habitat" / "lvl2_changemasks_ver004",
        output_dir=DATADIR / "habitat" / "current_raw",
    output:
        sentinel=DATADIR / "habitat" / "current_raw" / ".current_raw"
    threads: workflow.cores
    script:
        str(SRCDIR / "prepare_layers" / "make_current_map.py")

# =============================================================================
# Jung PNV map
# =============================================================================

rule jung_pnv_map:
    """
    Fetch the Jung PNV map from zenodo.
    """
    output:
        habitat=DATADIR / "habitat" / "pnv_raw.tif"
    params:
        zenodo_id=config["zenodo"]["jung_pnv"]["zenodo_id"],
        filename=config["zenodo"]["jung_pnv"]["filename"],
    log:
        DATADIR / "logs" / "download_pnv.log",
    shell:
        """
        reclaimer zenodo --zenodo_id {params.zenodo_id} \
                         --filename "{params.filename}" \
                         --extract \
                         --output {output.habitat} \
                         2>&1 | tee {log}
        """
