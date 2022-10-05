import argparse
import contextlib
import json
import sys
from typing import List, Set, Tuple

import geopandas as gpd
import pandas as pd

from aoh.lib import seasonality
from iucn_modlib.classes.Taxon import Taxon
from iucn_modlib.factories import TaxonFactories

@contextlib.contextmanager
def file_writer(file_name = None):
    writer = open(file_name, "w", encoding="utf-8") if file_name is not None else sys.stdout
    yield writer
    if file_name:
        writer.close()

def project_species_list(project: str, ranges: str) -> List[Tuple[int, str]]:
    ''' Returns a list of species that have ranges that intersect with project polygon
        To Check: does it give the correct answers? , What do we want the output to be like?
        (id_nos are float but should maybe be int)

        Parameters:
        Project: the file address of a project polygon
        Ranges: the file address of the species' range polygons

        Output: dictionary of id_no and binomial for species that are present
    '''
    # IMPORT PROJECT POLYGON
    project_polygon = gpd.read_file(project)
    # IMPORT SPECIES RANGES FILTERED BY WHETHER THEY INTERSECT WITH THE PROJECT POLYGON
    ranges_gdf = gpd.read_file(ranges, mask=project_polygon)
    # CONVERT TO DATAFRAME
    # Note: Not sure if all of these steps are necessary
    ranges_df = pd.DataFrame(ranges_gdf) # I think stops it being a spatial database?
    # EXTRACT A LIST OF UNIQUE ID_NO and UNIQUE BIOMIALS
    id_list = [int(x) for x in ranges_df['id_no'].unique().tolist()]
    binomial_list = ranges_df['binomial'].unique().tolist()
    return zip(id_list, binomial_list)

def seasonality_for_species(species: Taxon, range_file: str) -> Set[str]:
    og_seasons = set(
        seasonality.habitatSeasonality(species) +
        seasonality.rangeSeasonality(range_file, species.taxonid)
    )
    if len(og_seasons) == 0:
        return {}
    seasons = {'resident'}
    if len(og_seasons.difference({'resident'})) > 0:
        seasons = {'breeding', 'nonbreeding'}
    return seasons

def main() -> None:
    parser = argparse.ArgumentParser(description="Species and seasonality generator.")
    parser.add_argument(
        '--experiment',
        type=str,
        help="name of experiment group from configuration json",
        required=True,
        dest="experiment"
    )
    parser.add_argument(
        '--config',
        type=str,
        help="path of configuration json",
        required=False,
        dest="config_path",
        default="config.json"
    )
    parser.add_argument(
        '--project',
        type=str,
        help="name of project file geojson",
        required=True,
        dest="project"
    )
    parser.add_argument(
        '--output',
        type=str,
        help="name of output file for csv",
        required=False,
        dest="output"
    )
    parser.add_argument(
        '--epochs',
        type=str,
        help="comma seperated (but no spaces!) list of experiments to run for",
        required=True,
        dest="epochs"
    )
    args = vars(parser.parse_args())

    try:
        with open(args['config_path'], 'r', encoding='utf-8') as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        print(f'Failed to find configuration json file {args["config_path"]}')
        sys.exit(-1)
    except json.decoder.JSONDecodeError as exc:
        print(f'Failed to parse {args["config_path"]} at line {exc.lineno}, column {exc.colno}: {exc.msg}')
        sys.exit(-1)

    try:
        experiment = config['experiments'][args['experiment']]
    except KeyError:
        if not 'experiments' in config:
            print("No experiments section founnd in configuration json")
        else:
            print(f'Failed to find experiment with name {args["experiment"]}. Options found:')
            for experiment in config['experiments']:
                print(f'\t{experiment}')
        sys.exit(-1)

    epoch_list = args['epochs'].split(',')

    try:
        range_path = experiment['range']
    except KeyError:
        print(f'Experiment "{args["experiment"]}" was missing range key.')

    batch = None
    if 'iucn_batch' in experiment:
        batch = TaxonFactories.loadBatchSource(experiment['iucn_batch'])

    # Work part 1: get the species list
    species_list = project_species_list(args["project"], range_path)

    with file_writer(args["output"]) as output:
        output.write('--taxid,--seasonality,--experiment\n')
        for species_id, _ in species_list:
            if batch:
                # try:
                species = TaxonFactories.TaxonFactoryRedListBatch(species_id, batch)
                # except IndexError as e:
                #     # Some of the data in the batch needs tidy...
                #     print(f"Oh no {e}")
                #     continue
            else:
                try:
                    species = TaxonFactories.TaxonFactoryRedListAPI(species_id, config['iucn']['api_key'])
                except KeyError:
                    print("Failed to find IUCN API key in config file or batch path in experiment.")
                    sys.exit(-1)

            seasonality_list = seasonality_for_species(species, range_path)
            for season in seasonality_list:
                for epoch in epoch_list:
                    output.write(f'{species_id},{season},{epoch}\n')

if __name__ == "__main__":
    main()
