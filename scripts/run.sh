#!/bin/bash
#
# Assumes you've set up a python virtual environement in the current directory.
#
# In addition to the Python environemnt, you will need the following extra command line tools:
#
# https://github.com/quantifyearth/reclaimer - used to download inputs from Zenodo directly
# https://github.com/quantifyearth/littlejohn - used to run batch jobs in parallel

set -e
set -x

if [ -z "${DATADIR}" ]; then
    echo "Please specify $DATADIR"
    exit 1
fi


if [ -z "${VIRTUAL_ENV}" ]; then
    echo "Please specify run in a virtualenv"
    exit 1
fi

declare -a SCENARIOS=("arable" "restore" "restore_all" "urban" "pasture" "restore_agriculture")
declare -a TAXAS=("AMPHIBIA" "AVES" "MAMMALIA" "REPTILIA")
export CURVE=0.25
export PIXEL_SCALE=0.016666666666667

check_scenario() {
    local target="$1"
    for scenario in "${SCENARIOS[@]}"; do
        if [[ "$scenario" == "$target" ]]; then
            return 0
        fi
    done
    return 1
}

if [ ! -f "${DATADIR}"/crosswalk.csv ]; then
    python3 ./prepare_layers/generate_crosswalk.py --output "${DATADIR}"/crosswalk.csv
fi

# Get habitat layer and prepare for use
if [ ! -f "${DATADIR}"/habitat/current_raw.tif ]; then
    if [ ! -f "${DATADIR}"/habitat/jung_l2_raw.tif ]; then
        reclaimer zenodo --zenodo_id 4058819 \
                        --filename iucn_habitatclassification_composite_lvl2_ver004.zip \
                        --extract \
                        --output "${DATADIR}"/habitat/jung_l2_raw.tif
    fi

    if [ ! -d "${DATADIR}"/habitat/lvl2_changemasks_ver004 ]; then
        reclaimer zenodo --zenodo_id 4058819 \
                        --filename lvl2_changemasks_ver004.zip \
                        --extract \
                        --output "${DATADIR}"/habitat/
    fi

    python3 ./prepare_layers/make_current_map.py --jung "${DATADIR}"/habitat/jung_l2_raw.tif \
                    --update_masks "${DATADIR}"/habitat/lvl2_changemasks_ver004 \
                    --crosswalk "${DATADIR}"/crosswalk.csv \
                    --output "${DATADIR}"/habitat/current_raw.tif \
                    -j 16
fi

if [ ! -d "${DATADIR}"/habitat_maps/current ]; then
    aoh-habitat-process --habitat "${DATADIR}"/habitat/current_raw.tif \
                        --scale "${PIXEL_SCALE}" \
                        --output "${DATADIR}"/habitat_maps/current/
fi

# Get PNV layer and prepare for use
if [ ! -f "${DATADIR}"/habitat/pnv_raw.tif ]; then
    reclaimer zenodo --zenodo_id 4038749 \
                    --filename pnv_lvl1_004.zip \
                    --extract \
                    --output "${DATADIR}"/habitat/pnv_raw.tif
fi

if [ ! -d "${DATADIR}"/habitat_maps/pnv ]; then
    aoh-habitat-process --habitat "${DATADIR}"/habitat/pnv_raw.tif \
                        --scale "${PIXEL_SCALE}" \
                        --output "${DATADIR}"/habitat_maps/pnv/
fi

# Generate an area scaling map
if [ ! -f "${DATADIR}"/area-per-pixel.tif ]; then
    python3 ./prepare_layers/make_area_map.py --scale "${PIXEL_SCALE}" --output "${DATADIR}"/area-per-pixel.tif
fi

# Generate the arable scenario map
if check_scenario "arable"; then
    if [ ! -f "${DATADIR}"/habitat/arable.tif ]; then
        python3 ./prepare_layers/make_arable_map.py --current "${DATADIR}"/habitat/current_raw.tif \
                                        --output "${DATADIR}"/habitat/arable.tif
    fi

    if [ ! -d "${DATADIR}"/habitat_maps/arable ]; then
        aoh-habitat-process --habitat "${DATADIR}"/habitat/arable.tif \
                            --scale "${PIXEL_SCALE}" \
                            --output "${DATADIR}"/habitat_maps/arable/
    fi

    if [ ! -f "${DATADIR}"/habitat/arable_diff_area.tif ]; then
        python3 ./prepare_layers/make_diff_map.py --current "${DATADIR}"/habitat/current_raw.tif \
                                                --scenario "${DATADIR}"/habitat/arable.tif \
                                                --area "${DATADIR}"/area-per-pixel.tif \
                                                --scale "${PIXEL_SCALE}" \
                                                --output "${DATADIR}"/habitat/arable_diff_area.tif
    fi
fi

# Generate the pasture scenario map
if check_scenario "pasture"; then
    if [ ! -f "${DATADIR}"/habitat/pasture.tif ]; then
        python3 ./prepare_layers/make_pasture_map.py --current "${DATADIR}"/habitat/current_raw.tif \
                                                    --output "${DATADIR}"/habitat/pasture.tif
    fi

    if [ ! -d "${DATADIR}"/habitat_maps/pasture ]; then
        aoh-habitat-process --habitat "${DATADIR}"/habitat/pasture.tif \
                            --scale "${PIXEL_SCALE}" \
                            --output "${DATADIR}"/habitat_maps/pasture/
    fi

    if [ ! -f "${DATADIR}"/habitat/pasture_diff_area.tif ]; then
        python3 ./prepare_layers/make_diff_map.py --current "${DATADIR}"/habitat/current_raw.tif \
                                                --scenario "${DATADIR}"/habitat/pasture.tif \
                                                --area "${DATADIR}"/area-per-pixel.tif \
                                                --scale "${PIXEL_SCALE}" \
                                                --output "${DATADIR}"/habitat/pasture_diff_area.tif
    fi
fi

# Generate the restore map
if check_scenario "restore"; then
    if [ ! -f "${DATADIR}"/habitat/restore.tif ]; then
        python3 ./prepare_layers/make_restore_map.py --pnv "${DATADIR}"/habitat/pnv_raw.tif \
                                        --current "${DATADIR}"/habitat/current_raw.tif \
                                        --crosswalk "${DATADIR}"/crosswalk.csv \
                                        --output "${DATADIR}"/habitat/restore.tif
    fi

    if [ ! -d "${DATADIR}"/habitat_maps/restore ]; then
        aoh-habitat-process --habitat "${DATADIR}"/habitat/restore.tif \
                            --scale "${PIXEL_SCALE}" \
                            --output "${DATADIR}"/habitat_maps/restore/
    fi

    if [ ! -f "${DATADIR}"/habitat/restore_diff_area.tif ]; then
        python3 ./prepare_layers/make_diff_map.py --current "${DATADIR}"/habitat/current_raw.tif \
                                                --scenario "${DATADIR}"/habitat/restore.tif \
                                                --area "${DATADIR}"/area-per-pixel.tif \
                                                --scale "${PIXEL_SCALE}" \
                                                --output "${DATADIR}"/habitat/restore_diff_area.tif
    fi
fi

# Generate the restore_agriculture map
if check_scenario "restore_agriculture"; then
    if [ ! -f "${DATADIR}"/habitat/restore_agriculture.tif ]; then
        python3 ./prepare_layers/make_restore_agriculture_map.py --pnv "${DATADIR}"/habitat/pnv_raw.tif \
                                                                --current "${DATADIR}"/habitat/current_raw.tif \
                                                                --crosswalk "${DATADIR}"/crosswalk.csv \
                                                                --output "${DATADIR}"/habitat/restore_agriculture.tif
    fi

    if [ ! -d "${DATADIR}"/habitat_maps/restore_agriculture ]; then
        aoh-habitat-process --habitat "${DATADIR}"/habitat/restore_agriculture.tif \
                            --scale "${PIXEL_SCALE}" \
                            --output "${DATADIR}"/habitat_maps/restore_agriculture/
    fi

    if [ ! -f "${DATADIR}"/habitat/restore_agriculture_diff_area.tif ]; then
        python3 ./prepare_layers/make_diff_map.py --current "${DATADIR}"/habitat/current_raw.tif \
                                                --scenario "${DATADIR}"/habitat/restore_agriculture.tif \
                                                --area "${DATADIR}"/area-per-pixel.tif \
                                                --scale "${PIXEL_SCALE}" \
                                                --output "${DATADIR}"/habitat/restore_agriculture_diff_area.tif
    fi
fi

# Generate the restore all map
if check_scenario "restore_all"; then
    if [ ! -f "${DATADIR}"/habitat/restore_all.tif ]; then
        python3 ./prepare_layers/make_restore_all_map.py --pnv "${DATADIR}"/habitat/pnv_raw.tif \
                                        --current "${DATADIR}"/habitat/current_raw.tif \
                                        --crosswalk "${DATADIR}"/crosswalk.csv \
                                        --output "${DATADIR}"/habitat/restore_all.tif
    fi

    if [ ! -d "${DATADIR}"/habitat_maps/restore_all ]; then
        aoh-habitat-process --habitat "${DATADIR}"/habitat/restore_all.tif \
                            --scale "${PIXEL_SCALE}" \
                            --output "${DATADIR}"/habitat_maps/restore_all/
    fi

    if [ ! -f "${DATADIR}"/habitat/restore_all_diff_area.tif ]; then
        python3 ./prepare_layers/make_diff_map.py --current "${DATADIR}"/habitat/current_raw.tif \
                                                --scenario "${DATADIR}"/habitat/restore_all.tif \
                                                --area "${DATADIR}"/area-per-pixel.tif \
                                                --scale "${PIXEL_SCALE}" \
                                                --output "${DATADIR}"/habitat/restore_all_diff_area.tif
    fi
fi

# Generate urban all map
if check_scenario "urban"; then
    if [ ! -d "${DATADIR}"/habitat_maps/urban ]; then
        python3 ./prepare_layers/make_constant_habitat.py --examplar "${DATADIR}"/habitat_maps/arable/lcc_1401.tif \
                                                        --habitat_code 14.5 \
                                                        --crosswalk "${DATADIR}"/crosswalk.csv \
                                                        --output "${DATADIR}"/habitat_maps/urban
    fi

    if [ ! -f "${DATADIR}"/habitat/urban_diff_area.tif ]; then
        python3 ./prepare_layers/make_constant_diff_map.py --current "${DATADIR}"/habitat/current_raw.tif \
                                                        --habitat_code 14.5 \
                                                        --crosswalk "${DATADIR}"/crosswalk.csv \
                                                        --area "${DATADIR}"/area-per-pixel.tif \
                                                        --scale "${PIXEL_SCALE}" \
                                                        --output "${DATADIR}"/habitat/urban_diff_area.tif
    fi
fi

# Fetch and prepare the elevation layers
if [ ! -f "${DATADIR}"/elevation.tif ]; then
    reclaimer zenodo --zenodo_id 5719984  --filename dem-100m-esri54017.tif --output "${DATADIR}"/elevation.tif
fi
if [ ! -f "${DATADIR}"/elevation-max.tif ]; then
    gdalwarp -t_srs EPSG:4326 -tr "${PIXEL_SCALE}" -"${PIXEL_SCALE}" -r max -co COMPRESS=LZW -wo NUM_THREADS=40 "${DATADIR}"/elevation.tif "${DATADIR}"/elevation-max.tif
fi
if [ ! -f "${DATADIR}"/elevation-min.tif ]; then
    gdalwarp -t_srs EPSG:4326 -tr "${PIXEL_SCALE}" -"${PIXEL_SCALE}" -r min -co COMPRESS=LZW -wo NUM_THREADS=40 "${DATADIR}"/elevation.tif "${DATADIR}"/elevation-min.tif
fi

# Get species data per taxa from IUCN data
for TAXA in "${TAXAS[@]}"
do
    if [ ! -f "${DATADIR}"/overrides.csv ]; then
        python3 ./prepare_species/extract_species_psql.py --class "${TAXA}" \
                                                        --output "${DATADIR}"/species-info/"${TAXA}"/ \
                                                        --projection "EPSG:4326" \
                                                        --overrides "${DATADIR}"/overrides.csv
    else
        python3 ./prepare_species/extract_species_psql.py --class "${TAXA}" \
                                                        --output "${DATADIR}"/species-info/"${TAXA}"/ \
                                                        --projection "EPSG:4326"
    fi
done

# Generate the batch job input CSVs
python3 ./utils/speciesgenerator.py --datadir "${DATADIR}" \
                                    --output "${DATADIR}"/aohbatch.csv \
                                    --scenarios "${SCENARIOS[@]}"
python3 ./utils/persistencegenerator.py --datadir "${DATADIR}" \
                                        --curve "${CURVE}" \
                                        --output "${DATADIR}"/persistencebatch.csv \
                                        --scenarios "${SCENARIOS[@]}"

# Calculate all the AoHs
littlejohn -j 700 -o "${DATADIR}"/aohbatch.log -c "${DATADIR}"/aohbatch.csv aoh-calc -- --force-habitat

# Generate validation summaries
aoh-collate-data --aoh_results "${DATADIR}"/aohs/current/ --output "${DATADIR}"/aohs/current.csv
aoh-collate-data --aoh_results "${DATADIR}"/aohs/pnv/ --output "${DATADIR}"/aohs/pnv.csv
for SCENARIO in "${SCENARIOS[@]}"
do
    aoh-collate-data --aoh_results "${DATADIR}"/aohs/"${SCENARIO}"/ --output "${DATADIR}"/aohs/"${SCENARIO}".csv
done

# Calculate predictors from AoHs
aoh-species-richness --aohs_folder "${DATADIR}"/aohs/current/ \
                     --output "${DATADIR}"/predictors/species_richness.tif
aoh-endemism --aohs_folder "${DATADIR}"/aohs/current/ \
             --species_richness "${DATADIR}"/predictors/species_richness.tif \
             --output "${DATADIR}"/predictors/endemism.tif

# Calculate the per species Delta P values
littlejohn -j 200 -o "${DATADIR}"/persistencebatch.log -c "${DATADIR}"/persistencebatch.csv "${VIRTUAL_ENV}"/bin/python3 --  ./deltap/global_code_residents_pixel.py

for SCENARIO in "${SCENARIOS[@]}"
do
    for TAXA in "${TAXAS[@]}"
    do
        python3 ./utils/raster_sum.py --rasters_directory "${DATADIR}"/deltap/"${SCENARIO}"/"${CURVE}"/"${TAXA}"/ --output "${DATADIR}"/deltap_sum/"${SCENARIO}"/"${CURVE}"/"${TAXA}".tif
    done

    python3 ./utils/species_totals.py --deltaps "${DATADIR}"/deltap/"${SCENARIO}"/"${CURVE}"/ --output "${DATADIR}"/deltap/"${SCENARIO}"/"${CURVE}"/totals.csv

    # Generate final map
    python3 ./deltap/delta_p_scaled.py --input "${DATADIR}"/deltap_sum/"${SCENARIO}"/"${CURVE}"/ \
                                       --diffmap "${DATADIR}"/habitat/"${SCENARIO}"_diff_area.tif \
                                       --totals "${DATADIR}"/deltap/"${SCENARIO}"/"${CURVE}"/totals.csv \
                                       --output "${DATADIR}"/deltap_final/scaled_"${SCENARIO}"_"${CURVE}".tif
done
