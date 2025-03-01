#!/bin/bash
#
# Assumes you've set up a python virtual environement in the current directory.
#
# In addition to the Python environemnt, you will need the following extra command line tools:
#
# https://github.com/quantifyearth/reclaimer - used to download inputs from Zenodo directly
# https://github.com/quantifyearth/littlejohn - used to run batch jobs in parallel

set -e

if [ -z "${DATADIR}" ]; then
    echo "Please specify $DATADIR"
    exit 1
fi


if [ -z "${VIRTUAL_ENV}" ]; then
    echo "Please specify run in a virtualenv"
    exit 1
fi

declare -a CURVES=("0.1" "0.25" "0.5" "1.0" "gompertz")

python3 ./prepare_layers/generate_crosswalk.py --output ${DATADIR}/crosswalk.csv

# Get habitat layer and prepare for use
reclaimer zenodo --zenodo_id 4058819 \
                 --filename iucn_habitatclassification_composite_lvl2_ver004.zip \
                 --extract \
                 --output ${DATADIR}/habitat/jung_l2_raw.tif

python3 ./prepare-layers/make_current_map.py --jung /data/habitat/jung_l2_raw.tif \
                  --crosswalk /data/crosswalk.csv \
                  --output /data/habitat/current_raw.tif \
                  -j 16

python3 ./aoh-calculator/habitat_process.py --habitat ${DATADIR}/habitat/current_raw.tif \
                                            --scale 0.016666666666667 \
                                            --output ${DATADIR}/habitat_maps/current/

# Get PNV layer and prepare for use
reclaimer zenodo --zenodo_id 4038749 \
                 --filename pnv_lvl1_004.zip \
                 --extract \
                 --output ${DATADIR}/habitat/pnv_raw.tif

python3 ./aoh-calculator/habitat_process.py --habitat ${DATADIR}/habitat/pnv_raw.tif \
                                            --scale 0.016666666666667 \
                                            --output ${DATADIR}/habitat_maps/pnv/


# Generate an area scaling map
python3 ./prepare_layers/make_area_map.py --scale 0.016666666666667 --output ${DATADIR}/area-per-pixel.tif

# Generate the arable scenario map
python3 ./prepare_layers/make_arable_map.py --current ${DATADIR}/habitat/current_raw.tif \
                                  --output ${DATADIR}/habitat/arable.tif

python3 ./aoh-calculator/habitat_process.py --habitat ${DATADIR}/habitat/arable.tif \
                                            --scale 0.016666666666667 \
                                            --output ${DATADIR}/habitat_maps/arable/

python3 ./prepare_layers/make_diff_map.py --current ${DATADIR}/habitat/current_raw.tif \
                                          --scenario ${DATADIR}/habitat/arable.tif \
                                          --area ${DATADIR}/area-per-pixel.tif \
                                          --scale 0.016666666666667 \
                                          --output ${DATADIR}/habitat/arable_diff_area.tif

# Generate the restore map
python3 ./prepare_layers/make_restore_map.py --pnv ${DATADIR}/habitat/pnv_raw.tif \
                                   --current ${DATADIR}/habitat/current_raw.tif \
                                   --crosswalk ${DATADIR}/crosswalk.csv \
                                   --output ${DATADIR}/habitat/restore.tif

python3 ./aoh-calculator/habitat_process.py --habitat ${DATADIR}/habitat/restore.tif \
                                             --scale 0.016666666666667 \
                                             --output ${DATADIR}/habitat_maps/restore/

python3 ./prepare_layers/make_diff_map.py --current ${DATADIR}/habitat/current_raw.tif \
                                          --scenario ${DATADIR}/habitat/restore.tif \
                                          --area ${DATADIR}/area-per-pixel.tif \
                                          --scale 0.016666666666667 \
                                          --output ${DATADIR}/habitat/restore_diff_area.tif

# Fetch and prepare the elevation layers
reclaimer zenodo --zenodo_id 5719984  --filename dem-100m-esri54017.tif --output ${DATADIR}/elevation.tif
gdalwarp -t_srs EPSG:4326 -tr 0.016666666666667 -0.016666666666667 -r max -co COMPRESS=LZW -wo NUM_THREADS=40 ${DATADIR}/elevation.tif ${DATADIR}/elevation-max.tif
gdalwarp -t_srs EPSG:4326 -tr 0.016666666666667 -0.016666666666667 -r min -co COMPRESS=LZW -wo NUM_THREADS=40 ${DATADIR}/elevation.tif ${DATADIR}/elevation-min.tif

# Get species data per taxa from IUCN data
python3 ./prepare_species/extract_species_psql.py --class AVES --output ${DATADIR}/species-info/AVES/ --projection "EPSG:4326"
python3 ./prepare_species/extract_species_psql.py --class AMPHIBIA --output ${DATADIR}/species-info/AMPHIBIA/ --projection "EPSG:4326"
python3 ./prepare_species/extract_species_psql.py --class MAMMALIA --output ${DATADIR}/species-info/MAMMALIA/ --projection "EPSG:4326"
python3 ./prepare_species/extract_species_psql.py --class REPTILIA --output ${DATADIR}/species-info/REPTILIA/ --projection "EPSG:4326"

# Generate the batch job input CSVs
python3 ./utils/speciesgenerator.py --input ${DATADIR}/species-info --datadir ${DATADIR} --output ${DATADIR}/aohbatch.csv
python3 ./utils/persistencegenerator.py --input ${DATADIR}/species-info --datadir ${DATADIR} --output ${DATADIR}/persistencebatch.csv

# Calculate all the AoHs
littlejohn -j 200 -o ${DATADIR}/aohbatch.log -c ${DATADIR}/aohbatch.csv ${VIRTUAL_ENV}/bin/python3 -- ./aoh-calculator/aohcalc.py --force-habitat

# Calculate predictors from AoHs
python3 ./aoh-calculator/summaries/species_richness.py --aohs_folder ${DATADIR}/aohs/current/ \
                                                       --output ${DATADIR}/predictors/species_richness.tif
python3 ./aoh-calculator/summaries/endemism.py --aohs_folder ${DATADIR}/aohs/current/ \
                                               --species_richness ${DATADIR}/predictors/species_richness.tif \
                                               --output ${DATADIR}/predictors/endemism.tif

# Calculate the per species Delta P values
littlejohn -j 200 -o ${DATADIR}/persistencebatch.log -c ${DATADIR}/persistencebatch.csv ${VIRTUAL_ENV}/bin/python3 --  ./deltap/global_code_residents_pixel.py

for CURVE in "${CURVES[@]}"
do
    # Per scenario per taxa sum the delta Ps
    python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/arable/${CURVE}/REPTILIA/ --output ${DATADIR}/deltap_sum/arable/${CURVE}/REPTILIA.tif
    python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/arable/${CURVE}/AVES/ --output ${DATADIR}/deltap_sum/arable/${CURVE}/AVES.tif
    python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/arable/${CURVE}/MAMMALIA/ --output ${DATADIR}/deltap_sum/arable/${CURVE}/MAMMALIA.tif
    python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/arable/${CURVE}/AMPHIBIA/ --output ${DATADIR}/deltap_sum/arable/${CURVE}/AMPHIBIA.tif

    python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/restore/${CURVE}/MAMMALIA/ --output ${DATADIR}/deltap_sum/restore/${CURVE}/MAMMALIA.tif
    python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/restore/${CURVE}/AMPHIBIA/ --output ${DATADIR}/deltap_sum/restore/${CURVE}/AMPHIBIA.tif
    python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/restore/${CURVE}/REPTILIA/ --output ${DATADIR}/deltap_sum/restore/${CURVE}/REPTILIA.tif
    python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/restore/${CURVE}/AVES/ --output ${DATADIR}/deltap_sum/restore/${CURVE}/AVES.tif

    # Generate final map
    python3 ./deltap/delta_p_scaled_area.py --input ${DATADIR}/deltap_sum/restore/${CURVE}/ \
                                        --diffmap ${DATADIR}/habitat/restore_diff_area.tif \
                                        --output ${DATADIR}/deltap_final/scaled_restore_${CURVE}.tif

    python3 ./deltap/delta_p_scaled_area.py --input ${DATADIR}/deltap_sum/arable/${CURVE}/ \
                                        --diffmap ${DATADIR}/habitat/arable_diff_area.tif \
                                        --output ${DATADIR}/deltap_final/scaled_arable_${CURVE}.tif

    # Generate binary summaries for review
    python3 ./utils/binary_maps.py --input ${DATADIR}/deltap_final/scaled_restore_${CURVE}.tif \
                                    --output ${DATADIR}/binary/scaled_restore_${CURVE}.tif

    python3 ./utils/binary_maps.py --input ${DATADIR}/deltap_final/scaled_arable_${CURVE}.tif \
                                    --output ${DATADIR}/binary/scaled_arable_${CURVE}.tif
done

for CURVE in "${CURVES[@]}"
do
    if [ "${CURVE}" == "0.25" ]; then
        continue
    fi
    python3 ./utils/regression_plot.py --a ${DATADIR}/deltap_final/summed_scaled_arable_${CURVE}.tif \
                                    --b ${DATADIR}/deltap_final/summed_scaled_arable_0.25.tif \
                                    --output {$DATADIR}/analysis/arable_0.25_vs_${CURVE}.png

    python3 ./utils/regression_plot.py --a ${DATADIR}/deltap_final/summed_scaled_restore_${CURVE}.tif \
                                    --b ${DATADIR}/deltap_final/summed_scaled_restore_0.25.tif \
                                    --output {$DATADIR}/analysis/restore_0.25_vs_${CURVE}.png
done
