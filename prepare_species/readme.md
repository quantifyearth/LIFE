# Species selection for LIFE

## Methodology

LIFE is currently based on the following species selection criteria from the IUCN Redlist list of endangered species. In general we align with the guidelines set out in [Recent developments in the production of Area of Habitat (AOH) maps for terrestrial vertebrates.]() by Busana et al.

* We select from the classes AMPHIBIA, AVES, MAMMALIA, and REPTILIA.
* We exclude species that are categorised as:
    * Extinct
    * Not endangered
    * Data deficient
* Select the most recent asessment for each species.
* When selecting habitats we ignore those marked with their suitability as "marginal"
* For ranges
    * We select for origin as codes 1, 2 and 6 (Native, reintroduced, and assisted colonization)
    * LIFE generates data under both current and historic scenarios, and so the selection process for ranges is different for each scenario:
        * For current, we select under 1 and 2 (Extant and Probably Extant)
        * For historic, we select under 1, 2, 4, and 5 (Extant, Probably Extant, Possibly Extinct, and Extinct)
    * Seasonality is selecter from the categories Resident, Breeding, and non-Breeding. These are then combined in the following way:
        * For species with only a resident range, we treat them as resident only.
        * For species that are migratory (having either a breeding or non-breeding), we generate both a breeding and non-breeding range, where each is the union of the respective migratory range (if present) and the resident range if present.
* For metadata, we do the following hygine steps:
    * If elevation lower is missing, or less than the expected minimum we set it to that minimum: -500m
    * If elevation upper is missing, or over the expected maximum we set it to that maximum: 9000m
    * If the elevation lower is greater than the upper, we invert the two values
    * If the difference between the two values is less than 50m then each value is equally adjusted out from centre to ensure that they are 50m apart.

## Implementations

There are two implementations of the species extraction code, one that works with a download from the IUCN website, and one for those partnered/internal to the IUCN that works from the redlist website database directly.

### Downloads

From the IUCN redlist website you will need to download the species categories you are interested in, which will result in a ZIP file containing the following files:

* `all_other_fields.csv`
* `asessments.csv`
* `common_names.csv`
* `habitats.csv`
* `taxonomy.csv`

LIFE uses all of them except the `common_names.csv` file. You will provide to `extract_species_batch.py` the path of the folder containing these CSV files.

In addition, you need to provide the range files downloaded from the IUCN and optionally BirdLife also. At the time of writing these come as a set of ZIP files containing shape files split per taxa, so for convenience we first unify those into a single GPKG file by passing the zip files to `merge_range_downloads.py`, and that GPKG file is what we pass also to `extract_species_batch.py`.

### Database

If you are working inside the IUCN or as a close partner with access to a database containing the IUCN redlist data, you can use `extract_species_psql.py`. For this you will need to set the environmental variables `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD` to point to the instance of your PostGIS database.
