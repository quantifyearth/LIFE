import argparse
from pathlib import Path

import pandas as pd

def species_generator(
    data_dir: Path,
    output_csv_path: Path,
):
    species_info_dir = data_dir / "species-info"
    taxas = [x.name for x in species_info_dir.iterdir()]

    res = []
    for taxa in taxas:
        for scenario in ['current', 'restore', 'arable', 'pnv']:
        # for scenario in ['restore_all', 'urban']:
            source = 'historic' if scenario == 'pnv' else 'current'
            taxa_path = species_info_dir / taxa / source
            speciess = taxa_path.glob("*.geojson")
            for species in speciess:
                res.append([
                    data_dir / "habitat_maps" / scenario,
                    data_dir / "elevation-max.tif",
                    data_dir / "elevation-min.tif",
                    data_dir / "area-per-pixel.tif",
                    data_dir / "crosswalk.csv",
                    species,
                    data_dir / "aohs" / scenario / taxa,
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
        '--datadir',
        type=Path,
        help="directory for results",
        required=True,
        dest="data_dir",
    )
    parser.add_argument(
        '--output',
        type=Path,
        help="name of output file for csv",
        required=True,
        dest="output"
    )
    args = parser.parse_args()

    species_generator(args.data_dir, args.output)

if __name__ == "__main__":
    main()
