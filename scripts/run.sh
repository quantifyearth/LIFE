#!/bin/bash
#
# Assumes you've set up a python virtual environement in the current directory.
#
# In addition to the Python environemnt, you will need the following extra command line tools:
#
# https://github.com/quantifyearth/reclaimer - used to download inputs from Zenodo directly
# https://github.com/quantifyearth/littlejohn - used to run batch jobs in parallel


set -e

# Get habitat layer and prepare for use
reclaimer zenodo --zenodo_id 4058819 \
                 --filename iucn_habitatclassification_composite_lvl2_ver004.zip \
                 --extract \
                 --output ${DATADIR}/habitat/jung_l2_raw.tif

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
python3 ./prepare-layers/make_area_map.py --scale 0.016666666666667 --output ${DATADIR}/habitat/area-per-pixel.tif

# Generate the arable scenario map
python3 ./prepare-layers/make_arable_map.py --current ${DATADIR}/habitat/current_raw.tif \
                                  --crosswalk ${DATADIR}/crosswalk.csv \
                                  --output ${DATADIR}/habitat/arable.tif

python3 ./aoh-calculator/habitat_process.py --habitat ${DATADIR}/habitat/arable.tif \
                                            --scale 0.016666666666667 \
                                            --output ${DATADIR}/habitat_maps/arable/

python3 ./prepare-layers/make_diff_map.py --current ${DATADIR}/habitat/current_raw.tif \
                                          --scenario ${DATADIR}/habitat/restore.tif \
                                          --area ${DATADIR}/area-per-pixel.tif \
                                          --scale 0.016666666666667 \
                                          --output ${DATADIR}/habitat/restore_diff_area.tif

# Generate the restore map
python3 ./prepare-layers/make_restore_map.py --pnv ${DATADIR}/habitat/pnv_raw.tif \
                                   --current ${DATADIR}/habitat/current_raw.tif \
                                   --crosswalk ${DATADIR}/crosswalk.csv \
                                   --output ${DATADIR}/habitat/restore.tif

python3 ./aoh-calculator/habitat_process.py --habitat ${DATADIR}/habitat/restore.tif \
                                             --scale 0.016666666666667 \
                                             --output ${DATADIR}/habitat_maps/restore/

python3 ./prepare-layers/make_diff_map.py --current ${DATADIR}/habitat/current_raw.tif \
                                          --scenario ${DATADIR}/habitat/arable.tif \
                                          --area ${DATADIR}/area-per-pixel.tif \
                                          --scale 0.016666666666667 \
                                          --output ${DATADIR}/habitat/arable_diff_area.tif

# Fetch and prepare the elevation layers
reclaimer zenodo --zenodo_id 5719984  --filename dem-100m-esri54017.tif --output ${DATADIR}/elevation.tif
gdalwarp -t_srs EPSG:4326 -tr 0.016666666666667 -0.016666666666667 -r max -co COMPRESS=LZW -wo NUM_THREADS=40 ${DATADIR}/elevation.tif ${DATADIR}/elevation-max-1k.tif
gdalwarp -t_srs EPSG:4326 -tr 0.016666666666667 -0.016666666666667 -r min -co COMPRESS=LZW -wo NUM_THREADS=40 ${DATADIR}/elevation.tif ${DATADIR}/elevation-min-1k.tif

# Get species data per taxa from IUCN data
python3 ./prepare-species/extract_species_psql.py --class AVES --output ${DATADIR}/species-info/AVES/ --projection "EPSG:4326"
python3 ./prepare-species/extract_species_psql.py --class AMPHIBIA --output ${DATADIR}/species-info/AMPHIBIA/ --projection "EPSG:4326"
python3 ./prepare-species/extract_species_psql.py --class MAMMALIA --output ${DATADIR}/species-info/MAMMALIA/ --projection "EPSG:4326"
python3 ./prepare-species/extract_species_psql.py --class REPTILIA --output ${DATADIR}/species-info/REPTILIA/ --projection "EPSG:4326"

# Generate the batch job input CSVs
python3 ./utils/speciesgenerator.py --input ${DATADIR}/species-info --datadir ${DATADIR} --output ${DATADIR}/aohbatch.csv
python3 ./utils/persistencegenerator.py --input ${DATADIR}/species-info --datadir ${DATADIR} --output ${DATADIR}/persistencebatch.csv

# Calculate all the AoHs
littlejohn -j 200 -c ${DATADIR}/aohbatch.csv ${PWD}/venv/bin/python3 -- ./aoh-calculator/aohcalc.py --force-habitat

# Calculate the per species Delta P values
littlejohn -j 200 -c ${DATADIR}/persistencebatch.csv ${PWD}/venv/bin/python3 --  ./deltap/global_code_residents_pixel_AE_128.py

# Per scenario per taxa sum the delta Ps
python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/arable/0.25/REPTILIA/ --output ${DATADIR}/deltap_sum/arable/0.25/REPTILIA.tif
python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/arable/0.25/AVES/ --output ${DATADIR}/deltap_sum/arable/0.25/AVES.tif
python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/arable/0.25/MAMMALIA/ --output ${DATADIR}/deltap_sum/arable/0.25/MAMMALIA.tif
python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/arable/0.25/AMPHIBIA/ --output ${DATADIR}/deltap_sum/arable/0.25/AMPHIBIA.tif

python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/restore/0.25/MAMMALIA/ --output ${DATADIR}/deltap_sum/restore/0.25/MAMMALIA.tif
python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/restore/0.25/AMPHIBIA/ --output ${DATADIR}/deltap_sum/restore/0.25/AMPHIBIA.tif
python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/restore/0.25/REPTILIA/ --output ${DATADIR}/deltap_sum/restore/0.25/REPTILIA.tif
python3 ./utils/raster_sum.py --rasters_directory ${DATADIR}/deltap/restore/0.25/AVES/ --output ${DATADIR}/deltap_sum/restore/0.25/AVES.tif

# Generate final map
python3 ./deltap/delta_p_scaled_area.py --input ${DATADIR}/deltap_sum/restore/0.25/ \
                                    --diffmap ${DATADIR}/habitat/restore_diff_area.tif \
                                    --output ${DATADIR}/deltap_final/scaled_restore_0.25.tif

python3 ./deltap/delta_p_scaled_area.py --input ${DATADIR}/deltap_sum/arable/0.25/ \
                                    --diffmap ${DATADIR}/habitat/arable_diff_area.tif \
                                    --output ${DATADIR}/deltap_final/scaled_arable_0.25.tif
