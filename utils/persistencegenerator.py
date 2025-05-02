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
        taxa_path = os.path.join(input_dir, taxa, 'current')
        speciess = os.listdir(taxa_path)
        for scenario in ['urban', 'restore-all']:
            for species in speciess:
                # for curve in ["0.1", "0.25", "0.5", "1.0", "gompertz"]:
                for curve in ["0.25"]:
                    res.append([
                        os.path.join(data_dir, 'species-info', taxa, 'current', species),
                        os.path.join(data_dir, 'aohs', 'current', taxa),
                        os.path.join(data_dir, 'aohs', scenario, taxa),
                        os.path.join(data_dir, 'aohs', 'pnv', taxa),
                        curve,
                        os.path.join(data_dir, 'deltap', scenario, curve, taxa),
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
