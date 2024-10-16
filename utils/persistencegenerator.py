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
        taxa_path = os.path.join(input_dir, taxa, 'current')
        speciess = os.listdir(taxa_path)
        for scenario in ['arable', 'restore']:
            for species in speciess:
                res.append([
                    os.path.join('/home/mwd24/lifetest/species-info/', taxa, 'current', species),
                    os.path.join('/home/mwd24/lifetest/aohs/', 'current', taxa),
                    os.path.join('/home/mwd24/lifetest/aohs/', scenario, taxa),
                    os.path.join('/home/mwd24/lifetest/aohs/', 'pnv', taxa),
                    '0.25',
                    os.path.join('/home/mwd24/lifetest/deltap/', scenario, '0.25', taxa),
                ])


    df = pd.DataFrame(res, columns=[
        '--speciesdata',
        '--current_path',
        '--scenario_path',
        '--historic_path',
        '--z',
        '--output_path',
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
