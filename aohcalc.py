import argparse
import json
import os
import sys


import cProfile
import pstats

from iucn_modlib.factories import TaxonFactories

import persistence


parser = argparse.ArgumentParser(description="Area of habitat calculator.")
parser.add_argument(
    '--taxid',
    type=int,
    help="animal taxonomy id",
    required=True,
    dest="species"
)
parser.add_argument(
    '--seasonality',
    type=str,
    help="which season to calculate for (breeding, nonbreeding, or resident)",
    required=True,
    dest="seasonality"
)
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
    '--geotiffs',
    type=str,
    help='directory where area geotiffs should be stored',
    required=False,
    dest='results_path',
    default=None,
)
parser.add_argument(
    '--nogpu',
    type=str,
    help='disable CUDA usage',
    required=False,
    dest='nogpu',
    default='True',
)
parser.add_argument(
    '--profile',
    type=bool,
    help='enable profiling',
    required=False,
    dest='profile',
    default=False,
)
args = vars(parser.parse_args())

if args['nogpu'].lower() in ['t', 'true']:
    persistence.USE_GPU = False

try:
    seasonality = persistence.Seasonality(args['seasonality'])
except ValueError:
    print(f'Seasonality {args["seasonality"]} is not valid')
    sys.exit(-1)

try:
    with open(args['config_path'], 'r', encoding='utf-8') as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    print(f'Failed to find configuration json file {args["config_path"]}')
    sys.exit(-1)
except json.decoder.JSONDecodeError as e:
    print(f'Failed to parse {args["config_path"]} at line {e.lineno}, column {e.colno}: {e.msg}')
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

if 'iucn_batch' in experiment:
    batch = TaxonFactories.loadBatchSource(experiment['iucn_batch'])
    species = TaxonFactories.TaxonFactoryRedListBatch(args['species'], batch)
else:
    try:
        species = TaxonFactories.TaxonFactoryRedListAPI(args['species'], config['iucn']['api_key'])
    except KeyError:
        print("Failed to find IUCN API key in config file or batch path in experiment.")
        sys.exit(-1)

try:
    translator = experiment['translator']
    if translator == 'jung':
        TranslatorType = persistence.JungModel
    elif translator == 'esacci':
        TranslatorType = persistence.ESACCIModel
    else:
        print(f'Translator type of "{translator}" not recognised. Expected "jung" or "esacci".')
        sys.exit(-1)
except KeyError:
    print(f'Experiment {args["experiment"]} is missing a translator key')
    sys.exit(-1)

try:
    land = TranslatorType(
        experiment['habitat'],
        experiment['elevation'],
        experiment['area']
    )
except KeyError:
    print(f'Experiment "{args["experiment"]}" was missing one or more of the map keys: "habitat", "elevation", "area".')
    sys.exit(-1)

try:
    range_path = experiment['range']
except KeyError:
    print(f'Experiment "{args["experiment"]}" was missing range key.')

if args['results_path']:
    if not os.path.isdir(args['results_path']):
        print(f'Provided results path {args["results_path"]} is not a directory')
        sys.exit(-1)

if args['profile']:
    profiler = cProfile.Profile()
    profiler.enable()
else:
    profiler = None

try:
    result = persistence.calculator(species, range_path, land, seasonality, args['results_path'])
except KeyboardInterrupt:
    pass

if profiler:
    profiler.disable()
    p = pstats.Stats(profiler)
    p.sort_stats(pstats.SortKey.TIME).print_stats(20)

print(', '.join([str(x) for x in result]))
