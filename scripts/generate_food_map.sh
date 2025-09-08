#!/bin/bash
#
# This script will generate the updated habitat map for the FOOD paper based on the original Jung habitat map.
# Note that this is not derived directly from the Jung map, but rather from the simplified version used in LIFE,
# which has all habitats at level 1 except anthropomorphic ones at level 2. As such this script assumes you have
# downloaded and generated `current_raw.tif` from the original LIFE pipeline (see run.sh)

set -e

if [ -z "${DATADIR}" ]; then
    echo "Please specify $DATADIR"
    exit 1
fi

if [ -z "${VIRTUAL_ENV}" ]; then
    echo "Please specify run in a virtualenv"
    exit 1
fi

if [ ! -f "${DATADIR}"/habitat/current_raw.tif ]; then
    echo "LIFE Level 1 current map required"
    exit 1
fi

if [ ! -f "${DATADIR}"/habitat/pnv_raw.tif ]; then
    echo "Jung PNV map required"
    exit 1
fi

if [ ! -d "${DATADIR}"/food ]; then
    mkdir -p "${DATADIR}"/food
fi

# Get GAEZ data
if [ ! -f "${DATADIR}"/food/GLCSv11_02_5m.tif ]; then
    if [ ! -f "${DATADIR}"/food/LR.zip ]; then
        curl -o "${DATADIR}"/food/LR.zip https://s3.eu-west-1.amazonaws.com/data.gaezdev.aws.fao.org/LR.zip
    fi
    unzip -j "${DATADIR}"/food/LR.zip lco/GLCSv11_02_5m.tif -d "${DATADIR}"/food/GLCSv11_02_5m.tif
fi

# Get HYDE 3.2 data
if [ ! -f "${DATADIR}"/food/modified_grazing2017AD.asc ]; then
    if [ ! -f "${DATADIR}"/food/baseline.zip ]; then
        curl -o "${DATADIR}"/food/baseline.zip "https://geo.public.data.uu.nl/vault-hyde/HYDE%203.2%5B1710494848%5D/original/baseline.zip"
    fi
    if [ ! -f "${DATADIR}"/food/2017AD_lu.zip ]; then
        unzip -j "${DATADIR}"/food/baseline.zip zip/2017AD_lu.zip -d "${DATADIR}"/food/2017AD_lu.zip
    fi
    if [ ! -f "${DATADIR}"/food/grazing2017AD.asc ]; then
        unzip -j "${DATADIR}"/food/2017AD_lu.zip grazing2017AD.asc -d "${DATADIR}"/food/grazing2017AD.asc
    fi
    # The pixel scale in the two files have different rounding despite covering the same area
    # and so this makes them align.
    sed "s/0.0833333/0.08333333333333333/" "${DATADIR}"/food/grazing2017AD.asc > "${DATADIR}"/food/modified_grazing2017AD.asc
fi

# We need rescaled versions of the current data
python3 ./aoh-calculator/habitat_process.py --habitat "${DATADIR}"/habitat/current_raw.tif \
                                            --scale 0.08333333333333333 \
                                            --output "${DATADIR}"/food/current_layers/

# Combine GAEZ and HYDE data
python3 ./prepare_layers/build_gaez_hyde.py --gaez "${DATADIR}"/food/GLCSv11_02_5m.tif \
                                            --hyde "${DATADIR}"/food/modified_grazing2017AD.asc \
                                            --output "${DATADIR}"/food/

python3 ./utils/raster_diff.py --raster_a "${DATADIR}"/food/crop.tif \
                               --raster_b "${DATADIR}"/food/current_layers/lcc_1401.tif \
                               --output "${DATADIR}"/food/crop_diff.tif

python3 ./utils/raster_diff.py --raster_a "${DATADIR}"/food/pasture.tif \
                              --raster_b "${DATADIR}"/food/current_layers/lcc_1402.tif \
                              --output "${DATADIR}"/food/pasture_diff.tif

python3 ./prepare_layers/make_food_current_map.py --current_lvl1 "${DATADIR}"/habitat/current_raw.tif \
                                                  --pnv "${DATADIR}"/habitat/pnv_raw.tif \
                                                  --crop_diff "${DATADIR}"/food/crop_diff.tif \
                                                  --pasture_diff "${DATADIR}"/food/pasture_diff.tif \
                                                  --output "${DATADIR}"/food/current_raw.tif
