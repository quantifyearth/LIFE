#!/usr/bin/env python3

import argparse
import os

import pandas as pd

def species_generator(
    input_dir: str,
    data_dir: str,
    output_csv_path: str
):
    taxas = os.listdir(input_dir)

    res = []
    for taxa in taxas:
        # for scenario in ['current', 'restore', 'arable', 'pnv']:
        for scenario in ['restore_all', 'urban']:
            source = 'historic' if scenario == 'pnv' else 'current'
            taxa_path = os.path.join(input_dir, taxa, source)
            speciess = os.listdir(taxa_path)
            for species in speciess:
                res.append([
                    os.path.join(os.path.join(data_dir, "habitat_maps"), scenario),
                    os.path.join(data_dir, "elevation-max.tif"),
                    os.path.join(data_dir, "elevation-min.tif"),
                    os.path.join(data_dir, "area-per-pixel.tif"),
                    os.path.join(data_dir, "crosswalk.csv"),
                    os.path.join(os.path.join(data_dir, "species-info/"), taxa, source, species),
                    os.path.join(os.path.join(data_dir, "aohs/"), scenario, taxa)
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
        '--datadir',
        type=str,
        help="directory for results",
        required=True,
        dest="data_dir",
    )
    parser.add_argument(
        '--output',
        type=str,
        help="name of output file for csv",
        required=True,
        dest="output"
    )
    args = parser.parse_args()

    species_generator(args.input_dir, args.data_dir, args.output)

if __name__ == "__main__":
    main()
