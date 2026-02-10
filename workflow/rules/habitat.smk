# LIFE Pipeline - Updated habitat map rules
# ==========================================
#
# These rules handle combining the Jung habitat map with
# farming data from GAEZ and Hyde to generate a more suitable
# base map.


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
        curl -o {output.archive} \
            {params.url} \
            2>&1 | tee {log}
        """


rule gaez_expand:
    """
    Decompress the GAEZ download.
    """
    output:
        gaez_raster=DATADIR / "food" / "GLCSv11_02_5m.tif"
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
    """
    Fetch the compressed Hyde data.
    """
    output:
        archive=DATADIR / "food" / "baseline.zip"
    params:
        url="https://geo.public.data.uu.nl/vault-hyde/HYDE%203.2%5B1710494848%5D/original/baseline.zip",
    log:
        DATADIR / "logs" / "download_hyde.log",
    shell:
        """
        curl -o {output.archive} \
            {params.url} \
            2>&1 | tee {log}
        """


rule hyde_expand_land_usage_archive:
    """
    Extract the inner land usage archive.
    """
    output:
        inner_hyde=DATADIR / "food" / "2017AD_lu.zip"
    params:
        filepath="baseline/zip/2017AD_lu.zip"
    input:
        archive=DATADIR / "food" / "baseline.zip"
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
    """
    Extract raster from the inner land usage archive.
    """
    output:
        raw_hyde_raster=DATADIR / "food" / "grazing2017AD.asc"
    params:
        filepath="grazing2017AD.asc"
    input:
        archive=DATADIR / "food" / "2017AD_lu.zip"
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
    Modify the pixel scale value in the Hyde data as it isn't precise enough to match the GAEZ data.
    """
    output:
        modified_hyde_raster=DATADIR / "food" / "modified_grazing2017AD.asc"
    input:
        raw_hyde_raster=DATADIR / "food" / "grazing2017AD.asc"
    shell:
        """
        sed "s/0.0833333/0.08333333333333333/"  {input.raw_hyde_raster} > {output.modified_hyde_raster}
        """


rule add_hyde_projection_info:
    """
    The Hyde data ships without a projection specified, so we need to add one so that
    the rest of the workflow doesn't complain when we try to mix it in with other projected
    raster data.
    """
    output:
        hyde_projection_file=DATADIR / "food" / "modified_grazing2017AD.prj"
    shell:
        """
        echo {config["hyde_projection"]} > {output.hyde_projection_file}
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
# Project Jung to Hyde/GAEZ
# =============================================================================
rule project_jung_for_hyde_gaez:
    """
    We need a proportional value per pixel per habitat class version of the Jung
    current L1 map at the same scale as the GAEZ and Hyde data so we can then
    compare them.
    """
    input:
        habitat=ancient(DATADIR / "habitat" / "current_raw.tif"),
    output:
        crop=DATADIR / "food" / "current_layers" / "lcc_1401.tif",
        pasture=DATADIR / "food" / "current_layers" / "lcc_1402.tif",
    params:
        scale=config["hyde_pixel_scale"],
        output_dir=DATADIR / "food" / "current_layers",
    log:
        DATADIR / "logs" / "process_habitat_food.log",
    shell:
        """
        set -e
        aoh-habitat-process --habitat {input.habitat} \
                           --scale {params.scale} \
                           --projection "{params.projection}" \
                           --output {params.output_dir} \
                           2>&1 | tee {log}
        """


# =============================================================================
# Combine Jung, Hyde, and GAEZ
# =============================================================================
rule diff_crop:
    """
    Get the difference between the jung data and the new crop data.
    """
    input:
        raster_a=DATADIR / "food" / "crop.tif",
        raster_b=DATADIR / "food" / "current_layers" / "lcc_1401.tif",
    output:
        diff=DATADIR / "food" / "crop_diff.tif"
    script:
        str(SRCDIR / "utils" / "raster_diff.py")


rule diff_pasture:
    """
    Get the difference between the jung data and the new pasture data.
    """
    input:
        raster_a=DATADIR / "food" / "pasture.tif",
        raster_b=DATADIR / "food" / "current_layers" / "lcc_1402.tif",
    output:
        diff=DATADIR / "food" / "pasture_diff.tif"
    script:
        str(SRCDIR / "utils" / "raster_diff.py")


rule build_food_map:
    """
    Pull together all the pieces into a new L1 habitat map at the original Jung scale.
    """
    input:
        crop_diff=DATADIR / "food" / "crop_diff.tif",
        pasture_diff=DATADIR / "food" / "pasture_diff.tif",
        pnv=DATADIR / "habitat" / "pnv_raw.tif",
        jung=DATADIR / "habitat" / "current_raw.tif",
    output:
        raster=DATADIR / "food" / "current_raw.tif",
    params:
        seed=42
    threads: workflow.cores
    script:
        str(SRCDIR / "prepare_layers" / "make_food_current_map.py")
