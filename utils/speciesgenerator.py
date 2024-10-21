#!/usr/bin/env python3

import argparse
import os
from typing import List, Set

import pandas as pd

def species_generator(
    input_dir: str,
    output_csv_path: str
):
    taxas = os.listdir(input_dir)

    res = []
    for taxa in taxas:
        for scenario in ['current', 'restore', 'arable', 'pnv']:
            source = 'historic' if scenario == 'pnv' else 'current'
            taxa_path = os.path.join(input_dir, taxa, source)
            speciess = os.listdir(taxa_path)
            for species in speciess:
                res.append([
                    os.path.join('/home/mwd24/lifetest/habitat_maps', scenario),
                    '/home/mwd24/lifetest/elevation-max-1k.tif',
                    '/home/mwd24/lifetest/elevation-min-1k.tif',
                    '/home/mwd24/lifetest/area-per-pixel.tif',
                    '/home/mwd24/lifetest/crosswalk.csv',
                    os.path.join('/home/mwd24/lifetest/species-info/', taxa, source, species),
                    os.path.join('/home/mwd24/lifetest/aohs/', scenario, taxa)
                ])


    df = pd.DataFrame(res, columns=[
        '--habitats',
        '--elevation-max',
        '--elevation-min',
        '--area',
        '--crosswalk',
        '--speciesdata',
        '--output'
    ])
    df.to_csv(output_csv_path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Species and seasonality generator.")
    parser.add_argument(
        '--input',
        type=str,
        help="directory with taxa folders of species info",
        required=True,
        dest="input_dir"
    )
    parser.add_argument(
        '--output',
        type=str,
        help="name of output file for csv",
        required=False,
        dest="output"
    )
    args = parser.parse_args()

    species_generator(args.input_dir, args.output)

if __name__ == "__main__":
    main()