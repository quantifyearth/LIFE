import argparse
import os

import aoh

import persistence

ESACCI_PREFIX = 'S:\\aoh_trial\\esacci_aoh_basemaps'
JUNG_PREFIX = 'S:\\aoh_trial\\jung_aoh_basemaps'
PREFIX = '../../../aoh_trial'

parser = argparse.ArgumentParser(description="Area of habitat calculator.")
parser.add_argument('--taxid', type=int, help="animal taxonomy id", required=True, dest="species")
args = vars(parser.parse_args())

tax = aoh.lib.taxon.TaxonFactoryRedListAPI(args['species'], os.environ['IUCN_API_KEY'])

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

results = persistence.modeller(tax, range_path, land)

# output as CSV
for result in results:
    print(', '.join([str(x) for x in result]))
