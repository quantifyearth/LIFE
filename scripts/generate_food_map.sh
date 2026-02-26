#!/bin/bash
#
# This script will generate the updated habitat map for the FOOD paper based on the original Jung habitat map.
# Note that this is not derived directly from the Jung map, but rather from the simplified version used in LIFE,
# which has all habitats at level 1 except anthropomorphic ones at level 2. As such this script assumes you have
# downloaded and generated `current_raw.tif` from the original LIFE pipeline (see run.sh)

set -e
set -x


# We know we use two Go tools, so add go/bin to our path as in slurm world they're likely
# to be installed locally
export PATH="${PATH}":"${HOME}"/go/bin
if ! hash reclaimer 2>/dev/null; then
    echo "Please ensure reclaimer is available"
    exit 1
fi

# Detect if we're running under SLURM
if [[ -n "${SLURM_JOB_ID}" ]]; then
    # Slurm users will probably need to customise this
    # shellcheck disable=SC1091
    source "${HOME}"/venvs/life/bin/activate
    cd "${HOME}"/dev/life
    PROCESS_COUNT="${SLURM_JOB_CPUS_PER_NODE}"
else
    PROCESS_COUNT=$(nproc --all)
fi

if [ -z "${DATADIR}" ]; then
    echo "Please specify $DATADIR"
    exit 1
fi

if [ -z "${VIRTUAL_ENV}" ]; then
    echo "Please specify run in a virtualenv"
    exit 1
fi

if [ ! -f "${DATADIR}"/100m/jung_current/.sentinel ]; then
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

   if [ ! -f "${DATADIR}"/100m/jung_current/.sentinel ]; then
       python3 ./prepare_layers/make_current_map.py --jung_l2 "${DATADIR}"/habitat/jung_l2_raw.tif \
                       --update_masks "${DATADIR}"/habitat/lvl2_changemasks_ver004 \
                       --crosswalk "${DATADIR}"/crosswalk.csv \
                       --output "${DATADIR}"/100m/jung_current \
                       --sentinel "${DATADIR}"/100m/jung_current/.sentinel \
                       -j "${PROCESS_COUNT}"
   fi
fi

if [ ! -f "${DATADIR}"/habitat/pnv_raw.tif ]; then
    reclaimer zenodo --zenodo_id 4038749 \
                    --filename pnv_lvl1_004.zip \
                    --extract \
                    --output "${DATADIR}"/habitat/pnv_raw.tif
fi

if [ ! -f "${DATADIR}"/habitat/pnv_100m.tif ]; then
    # In theory we don't need to do this, as Yirgacheffe can rescale dynamically
    # but in practice it's faster if we just do this once like this, at the cost
    # of some extra storage requirements
    gdal -tr 0.000898315284120 -0.000898315284120 \
        -r near \
        -tap \
        -multi -wo NUM_THREADS=ALL_CPUS \
        -co COMPRESS=LZW -co NUM_THREADS=ALL_CPUS \
        "${DATADIR}"/habitat/pnv_raw.tif
        "${DATADIR}"/habitat/pnv_100m.tif
fi

# Get GAEZ data
if [ ! -f "${DATADIR}"/habitat/GLCSv11_02_5m.tif ]; then
    if [ ! -f "${DATADIR}"/habitat/LR.zip ]; then
        curl -o "${DATADIR}"/habitat/LR.zip https://s3.eu-west-1.amazonaws.com/data.gaezdev.aws.fao.org/LR.zip
    fi
    unzip -j "${DATADIR}"/habitat/LR.zip LR/lco/GLCSv11_02_5m.tif -d "${DATADIR}"/habitat/
fi

# Get HYDE 3.2 data
if [ ! -f "${DATADIR}"/habitat/modified_grazing2017AD.asc ]; then
    if [ ! -f "${DATADIR}"/habitat/baseline.zip ]; then
        curl -o "${DATADIR}"/habitat/baseline.zip "https://geo.public.data.uu.nl/vault-hyde/HYDE%203.2%5B1710494848%5D/original/baseline.zip"
    fi
    if [ ! -f "${DATADIR}"/habitat/2017AD_lu.zip ]; then
        unzip -j "${DATADIR}"/habitat/baseline.zip baseline/zip/2017AD_lu.zip -d "${DATADIR}"/habitat/
    fi
    if [ ! -f "${DATADIR}"/habitat/grazing2017AD.asc ]; then
        unzip -j "${DATADIR}"/habitat/2017AD_lu.zip grazing2017AD.asc -d "${DATADIR}"/habitat/
    fi
    # The pixel scale in the two files have different rounding despite covering the same area
    # and so this makes them align.
    sed "s/0.0833333/0.08333333333333333/" "${DATADIR}"/habitat/grazing2017AD.asc > "${DATADIR}"/habitat/modified_grazing2017AD.asc
fi

if [ ! -f "${DATADIR}"/habitat/modified_grazing2017AD.prj ]; then
    echo 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]' > "${DATADIR}"/habitat/modified_grazing2017AD.prj
fi

if [ ! -f "${DATADIR}"/habitat/crop.tif ] || [ ! -f "${DATADIR}"/habitat/pasture.tif ]; then
    python3 ./prepare_layers/build_gaez_hyde.py --gaez "${DATADIR}"/habitat/GLCSv11_02_5m.tif \
                                                --hyde "${DATADIR}"/habitat/modified_grazing2017AD.asc \
                                                --output "${DATADIR}"/habitat/
fi

if [ ! -f "${DATADIR}"/habitat/current_raw.tif ]; then
    python3 ./prepare_layers/make_food_current_map.py --current_lvl1 "${DATADIR}"/100m/jung_current \
                                                    --pnv "${DATADIR}"/habitat/pnv_100m.tif \
                                                    --crop "${DATADIR}"/habitat/crop.tif \
                                                    --pasture "${DATADIR}"/habitat/pasture.tif \
                                                    --output "${DATADIR}"/100m/current
fi
