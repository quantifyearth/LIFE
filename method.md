---
path: /root
TAXA:
- AMPHIBIA
- AVES
CURVE:
- "0.25"
---

# How to run the pipeline for LIFE

From [Eyres et al](https://www.cambridge.org/engage/coe/article-details/65801ab4e9ebbb4db92dad33).

## Build the environment

### The geospatial compute container

The dockerfile that comes with the repo should be used to run the pipeline.

```
docker build buildx --tag aohbuilder .
```

For use with the [shark pipeline](https://github.com/quantifyearth/shark), we need this block to trigger a build currently:

```shark-build:aohbuilder
((from ghcr.io/osgeo/gdal:ubuntu-small-3.8.5)
 (run (network host) (shell "apt-get update -qqy && apt-get -y install python3-pip libpq-dev git && rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*"))
 (run (network host) (shell "pip install --upgrade pip"))
 (run (network host) (shell "pip install 'numpy<2'"))
 (run (network host) (shell "pip install gdal[numpy]==3.8.5"))
 (run (shell "mkdir -p /root"))
 (workdir "/root")
 (copy (src "requirements.txt") (dst "./"))
 (copy (src "aoh-calculator") (dst "./"))
 (run (network host) (shell "pip install --no-cache-dir -r requirements.txt"))
)
```

For the primary data sources we fetch them directly from Zenodo/GitHub to allow for obvious provenance.

```shark-build:reclaimer
((from carboncredits/reclaimer:latest))
```

For the projection changes we use a barebones GDAL container. The reason for this is that these operations are expensive, and we don't want to re-execute them if we update our code.

```shark-build:gdalonly
((from ghcr.io/osgeo/gdal:ubuntu-small-3.8.5))
```

Alternatively you can build your own python virtual env assuming you have everything required. For this you will need at least a GDAL version installed locally, and you may want to update requirements.txt to match the python GDAL bindings to the version you have installed.

```
python3 -m virtualenv ./venv
. ./venv/bin/activate
pip install -r requirements.txt
```

### The PostGIS container

For querying the IUCN data held in the PostGIS database we use a seperate container, based on the standard PostGIS image. This does not run the database, rather is a place to run python scripts that will talk to the database.

```shark-build:postgis
((from python:3.12-slim)
 (run (network host) (shell "apt-get update -qqy && apt-get -y install libpq-dev gcc git && rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*"))
 (run (network host) (shell "pip install psycopg2 postgis geopandas"))
 (run (network host) (shell "pip install git+https://github.com/quantifyearth/pyshark"))
 (copy (src "./prepare-species") (dst "/root/"))
 (workdir "/root/")
)
```

## Fetching the required data

```shark-build:layer-prep
((from ghcr.io/osgeo/gdal:ubuntu-small-3.8.5)
 (run (network host) (shell "apt-get update -qqy && apt-get -y install python3-pip libpq-dev git && rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*"))
 (run (network host) (shell "pip install --upgrade pip"))
 (run (network host) (shell "pip install 'numpy<2'"))
 (run (network host) (shell "pip install gdal[numpy]==3.8.5"))
 (run (shell "mkdir -p /root"))
 (workdir "/root")
 (copy (src "requirements.txt") (dst "./"))
 (copy (src "aoh-calculator") (dst "./"))
 (run (network host) (shell "pip install --no-cache-dir -r requirements.txt"))
 (copy (src "prepare-layers") (dst "./"))
)
```

To calculate the AoH we need various basemaps:

- Habitat maps for four scenarios:
  - Current day, in both L1 and L2 IUCN habitat classification
  - Potential Natural Vegetation (PNV) showing the habitats predicted without human intevention
  - Restore scenario - a map derived from the PNV and current maps showing certain lands restored to their pre-human type
  - Conserve scenario - a map derived form current indicating the impact of placement of arable lands
- The Digital Elevation Map (DEM) which has the height per pixel in meters

All these maps must be at the same pixel spacing and projection, and the output AoH maps will be at that same pixel resolution and projection.

Habitat maps store habitat types in int types typically, the IUCN range data for species are of the form 'x.y' or 'x.y.z', and so you will need to also get a crosswalk table that maps between the IUCN ranges for the species and the particular habitat map you are using.

### Fetching the habitat maps

LIFE uses the work of Jung et al to get both the [current day habitat map](https://zenodo.org/records/4058819) and the [PNV habitat map](https://zenodo.org/records/4038749).

To assist with provenance, we download the data from the Zenodo ID.

```shark-run:reclaimer
reclaimer zenodo --zenodo_id 4038749 \
                 --filename pnv_lvl1_004.zip \
                 --extract \
                 --output /data/habitat/pnv_raw.tif
reclaimer zenodo --zenodo_id 4058819 \
                 --filename iucn_habitatclassification_composite_lvl2_ver004.zip \
                 --extract \
                 --output /data/habitat/jung_l2_raw.tif
```

For LIFE the crosswalk table is generated using code by Daniele Baisero's [IUCN Modlib](https://gitlab.com/daniele.baisero/iucn-modlib/) package:

```shark-run:layer-prep
python3 ./prepare-layers/generate_crosswalk.py --output /data/crosswalk.csv
```

The PNV map is only classified at Level 1 of the IUCN habitat codes, and so to match this non-artificial habitats in the L2 map are converted, as per Eyres et al:

| The current layer maps IUCN level 1 and 2 habitats, but habitats in the PNV layer are mapped only at IUCN level 1, so to estimate species’ proportion of original AOH now remaining we could only use natural habitats mapped at level 1 and artificial habitats at level 2.

```shark-run:layer-prep
python3 ./prepare-layers/make_current_map.py --jung /data/habitat/jung_l2_raw.tif \
                                             --crosswalk /data/crosswalk.csv \
                                             --output /data/habitat/current_raw.tif \
                                             -j 16
```

The habitat maps by Jung et al is at 100m per pixel at the equator resolution in WGS84 projection, which we covert to a series of downsampled proportional-coverage layers, one layer per habitat class, at our target result scale. This is done with `habtiat_process.py`:

```shark-run:layer-prep
python3 ./aoh-calculator/habitat_process.py --habitat /data/habitat/pnv_raw.tif \
                                            --scale 0.016666666666667 \
                                            --output /data/habitat_maps/pnv/
```

```shark-run:layer-prep
python3 ./aoh-calculator/habitat_process.py --habitat /data/habitat/current_raw.tif \
                                            --scale 0.016666666666667 \
                                            --output /data/habitat_maps/current/
```

This process to generate a proprotional layer per habitat class will be repereated for the additional layers generated below.

### Generating additional habitat maps

LIFE calculates the impact on extinction rates under two future scenarios: restoration of habitats to their pre-human state, and the converstion of non-urban terrestrial habitat to arable.

The definition of the restore layer from Section 5 of [Eyres et al](https://www.cambridge.org/engage/coe/article-details/65801ab4e9ebbb4db92dad33) is:

| In the restoration scenario all areas classified as arable or pasture were restored to their PNV.

```shark-run:layer-prep
python3 ./prepare-layers/make_restore_map.py --pnv /data/habitat/pnv_raw.tif \
                                   --current /data/habitat/current_raw.tif \
                                   --crosswalk /data/crosswalk.csv \
                                   --output /data/habitat/restore.tif

 python3 ./aoh-calculator/habitat_process.py --habitat /data/habitat/restore.tif \
                                             --scale 0.016666666666667 \
                                             --output /data/habitat_maps/restore/
```

The definition of the arable layer from Section 5 of [Eyres et al](https://www.cambridge.org/engage/coe/article-details/65801ab4e9ebbb4db92dad33) is:

| In the conversion scenario all habitats currently mapped as natural or pasture were converted to arable land.

```shark-run:layer-prep
python3 ./prepare-layers/make_arable_map.py --current /data/habitat/current_raw.tif \
                                  --crosswalk /data/crosswalk.csv \
                                  --output /data/habitat/arable.tif

python3 ./aoh-calculator/habitat_process.py --habitat /data/habitat/arable.tif \
                                            --scale 0.016666666666667 \
                                            --output /data/habitat_maps/arable/
```

### Generate area map

For LIFE we need to know the actual area, not just pixel count. For this we generate a raster map that contains the area per pixel in meters for a given latitude. As a performance optimisation, this map is generated as a one pixel wide raster, as the values at all latitudes are the same.

```shark-run:layer-prep
python3 ./prepare-layers/make_area_map.py --scale 0.016666666666667 --output /data/area-per-pixel.tif
```

### Differences maps

In the algorithm we use need to account for map projection distortions, so all values in the AoHs are based on the area per pixel. To get the final extinction risk values we must remove that scaling. To do that we generate a map of area difference from current for the given scenario.

```shark-run:layer-prep
python3 ./prepare-layers/make_diff_map.py --current /data/habitat/current_raw.tif \
                                          --scenario /data/habitat/restore.tif \
                                          --area /data/area-per-pixel.tif \
                                          --scale 0.016666666666667 \
                                          --output /data/habitat/restore_diff_area.tif
```

```shark-run:layer-prep
python3 ./prepare-layers/make_diff_map.py --current /data/habitat/current_raw.tif \
                                          --scenario /data/habitat/arable.tif \
                                          --area /data/area-per-pixel.tif \
                                          --scale 0.016666666666667 \
                                          --output /data/habitat/arable_diff_area.tif
```

### Fetching the elevation map

To calculate AoH we need a digital elevation map. We use [this map](https://zenodo.org/records/5719984) map compiled by Jeffrey Hanson.

```shark-run:reclaimer
reclaimer zenodo --zenodo_id 5719984  --filename dem-100m-esri54017.tif --output /data/elevation.tif
```

Similarly to the habitat map we need to downsample to the target projection, however rather than picking the mean elevation, we select both the min and max elevation for each pixel, and then check whether the species is in that range when we calculate AoH.

```shark-run:gdalonly
gdalwarp -t_srs EPSG:4326 -tr 0.016666666666667 -0.016666666666667 -r min -co COMPRESS=LZW -wo NUM_THREADS=40 /data/elevation.tif /data/elevation-min-1k.tif
gdalwarp -t_srs EPSG:4326 -tr 0.016666666666667 -0.016666666666667 -r max -co COMPRESS=LZW -wo NUM_THREADS=40 /data/elevation.tif /data/elevation-max-1k.tif
```

## Calculating AoH

Once all the data has been collected, we can now calclate the AoH maps.

### Get per species range data

Rather than calculate from the postgis database directly, we first split out the data into a single GeoJSON file per species per season:

```shark-run:postgis
export DB_HOST=somehost
export DB_USER=username
export DB_PASSWORD=secretpassword
export DB_NAME=iucnredlist

python3 ./prepare-species/extract_species_psql.py --class %{TAXA} --output /data/species-info/%{TAXA}/ --projection "EPSG:4326"
```

The reason for doing this primarly one of pipeline optimisation, though it also makes the tasks of debugging and provenance tracing much easier. Most build systems, including the one we use, let you notice when files have updated and only do the work required based on that update. If we have many thousands of species on the redlise and only a few update, if we base our calculation on a single file with all species in, we'll have to calculate all thousands of results. But with this step added in, we will re-generate the per species per season GeoJSON files, which is cheap, but then we can spot that most of them haven't changed and we don't need to then calculate the rasters for those ones in the next stage.

### Calculate AoH

This step generates a single AoH raster for a single one of the above GeoJSON files.

```shark-run:aohbuilder
python3 ./aoh-calculator/aohcalc.py --habitats /data/habitat_maps/current/ \
                                    --elevation-max /data/elevation-max-1k.tif \
                                    --elevation-min /data/elevation-min-1k.tif \
                                    --area /data/area-per-pixel.tif \
                                    --crosswalk /data/crosswalk.csv \
                                    --speciesdata /data/species-info/%{TAXA}/current/* \
                                    --output /data/aohs/current/%{TAXA}/

python3 ./aoh-calculator/aohcalc.py --habitats /data/habitat_maps/restore/ \
                                    --elevation-max /data/elevation-max-1k.tif \
                                    --elevation-min /data/elevation-min-1k.tif \
                                    --area /data/area-per-pixel.tif \
                                    --crosswalk /data/crosswalk.csv \
                                    --speciesdata /data/species-info/%{TAXA}/current/* \
                                    --output /data/aohs/restore/%{TAXA}/

python3 ./aoh-calculator/aohcalc.py --habitats /data/habitat_maps/arable/ \
                                    --elevation-max /data/elevation-max-1k.tif \
                                    --elevation-min /data/elevation-min-1k.tif \
                                    --area /data/area-per-pixel.tif \
                                    --crosswalk /data/crosswalk.csv \
                                    --speciesdata /data/species-info/%{TAXA}/current/* \
                                    --output /data/aohs/arable/%{TAXA}/

python3 ./aoh-calculator/aohcalc.py --habitats /data/habitat_maps/pnv/ \
                                    --elevation-max /data/elevation-max-1k.tif \
                                    --elevation-min /data/elevation-min-1k.tif \
                                    --area /data/area-per-pixel.tif \
                                    --crosswalk /data/crosswalk.csv \
                                    --speciesdata /data/species-info/%{TAXA}/historic/* \
                                    --output /data/aohs/pnv/%{TAXA}/
```

The results you then want will all be in:

```shark-publish2
/data/aohs/current/
/data/aohs/restore/
/data/aohs/arable/
/data/aohs/pnv/
```

## Calculating persistence maps


```shark-build:deltap
((from ghcr.io/osgeo/gdal:ubuntu-small-3.8.5)
 (run (network host) (shell "apt-get update -qqy && apt-get -y install python3-pip libpq-dev git && rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*"))
 (run (network host) (shell "pip install --upgrade pip"))
 (run (network host) (shell "pip install 'numpy<2'"))
 (run (network host) (shell "pip install gdal[numpy]==3.8.5"))
 (run (shell "mkdir -p /root"))
 (workdir "/root")
 (copy (src "requirements.txt") (dst "./"))
 (copy (src "aoh-calculator") (dst "./"))
 (run (network host) (shell "pip install --no-cache-dir -r requirements.txt"))
  (copy (src "deltap") (dst "./"))
  (copy (src "utils") (dst "./"))
)
```

For each species we use the AoH data to calculate the likelihood of extinction under two scenarios: restoration and conseravation. To do that we work out the delta_p value per species, and then sum together all those results per species into a single layer.


```shark-run:deltap
python3 ./deltap/global_code_residents_pixel.py --speciesdata /data/species-info/%{TAXA}/current/* \
                                                --current_path /data/aohs/current/%{TAXA}/ \
                                                --scenario_path /data/aohs/restore/%{TAXA}/ \
                                                --historic_path /data/aohs/pnv/%{TAXA}/ \
                                                --z %{CURVE} \
                                                --output_path /data/deltap/restore/%{CURVE}/%{TAXA}/

python3 ./utils/raster_sum.py --rasters_directory /data/deltap/restore/%{CURVE}/%{TAXA}/ --output /data/deltap_sum/restore/%{CURVE}/%{TAXA}.tif

python3 ./deltap/global_code_residents_pixel --speciesdata /data/species-info/%{TAXA}/current/* \
                                             --current_path /data/aohs/current/%{TAXA}/ \
                                             --scenario_path /data/aohs/arable/%{TAXA}/ \
                                             --historic_path /data/aohs/pnv/%{TAXA}/ \
                                             --z %{CURVE} \
                                             --output_path /data/deltap/arable/%{CURVE}/%{TAXA}/

python3 ./utils/raster_sum.py --rasters_directory /data/deltap/arable/%{CURVE}/%{TAXA}/ --output /data/deltap_sum/arable/%{CURVE}/%{TAXA}.tif
```

```shark-publish2
/data/deltap/restore/
/data/deltap/arable/
```

Finally, we need to scale the results for publication:

```shark-run:deltap
python3 ./deltap/delta_p_scaled_area.py --input /data/deltap_sum/restore/%{CURVE}/ \
                                        --diffmap /data/habitat/restore_diff_area.tif \
                                        --output /data/deltap_final/scaled_restore_%{CURVE}.tif

python3 ./deltap/delta_p_scaled_area.py --input /data/deltap_sum/arable/%{CURVE}/ \
                                        --diffmap /data/habitat/arable_diff_area.tif \
                                        --output /data/deltap_final/scaled_arable_%{CURVE}.tif
```

```shark-publish
/data/deltap_final/scaled_restore_%{CURVE}.tif
/data/deltap_final/scaled_arable_%{CURVE}.tif
```
