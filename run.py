import os
import sys

import aoh
import iucn_modlib.translator

import persistence

ESACCI_PREFIX = 'S:\\aoh_trial\\esacci_aoh_basemaps'
JUNG_PREFIX = 'S:\\aoh_trial\\jung_aoh_basemaps'
PREFIX = '../../../aoh_trial'

tax = aoh.lib.taxon.TaxonFactoryRedListAPI(int(sys.argv[1]), os.environ['IUCN_API_KEY'])

vt = persistence.JungModel(
    'no_nan/brasil_coverage_2020crop_NA.tif',
    'no_nan/elev_br_NA.tif',
    'no_nan/pixel_area_NA.tif'
)

ali = persistence.ESACCIModel(
    os.path.join(ESACCI_PREFIX, 'esacci_2020.tif'),
    os.path.join(ESACCI_PREFIX, 'esacci_dem.tif'),
    os.path.join(ESACCI_PREFIX, 'small_area.tif'),
)

land = ali

range_path = os.path.join(PREFIX, "mammals_terrestrial_filtered_collected_fix.gpkg")

habitatSeasons = aoh.lib.seasonality.habitatSeasonality(tax)
rangeSeasons = aoh.lib.seasonality.rangeSeasonality(range_path, tax.taxonid)
seasons = list(set(habitatSeasons + rangeSeasons))
if len(seasons) == 3:
    seasons = ('breeding', 'nonbreeding')
elif len(seasons) == 2 and 'resident' in seasons:
    seasons = ('breeding', 'nonbreeding')

elevation_range = (tax.elevation_lower, tax.elevation_upper)
habitat_params = iucn_modlib.ModelParameters(
    habMap = None,
    translator = land.translator,
    season = ('Resident', 'Seasonal Occurrence Unknown'),
    suitability = ('Suitable', 'Unknown'),
    majorImportance = ('Yes', 'No'),
)

for season in seasons:
    where_filter =  f"id_no = {tax.taxonid} and season in ('{season}', 'resident')"

    if season == 'resident':
        habitat_params.season = ('Resident', 'Seasonal Occurrence Unknown')
    elif season == 'breeding':
        habitat_params.season = ('Resident', 'Breeding Season', 'Seasonal Occurrence Unknown')
    elif season == 'nonbreedng':
        habitat_params.seasons = ('Resident', 'Non-Breeding Season', 'Seasonal Occurrence Unknown'),
    else:
        raise ValueError(f'Unexpected season {season}')

    habitat_list = tax.habitatCodes(habitat_params)

    result = persistence.modeller(
        range_path,
        where_filter,
        land.landc,
        habitat_list,
        land.dem,
        elevation_range,
        land.area
    )
    print(tax.taxonid, season, result)

