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
        taxa_path = species_info_dir / taxa / "current"
        speciess = list(taxa_path.glob("*.geojson"))
        for scenario in ['arable', 'restore']:
            for species in speciess:
                # for curve in ["0.1", "0.25", "0.5", "1.0", "gompertz"]:
                for curve in ["0.25"]:
                    res.append([
                        species,
                        data_dir / "aohs" / "current" / taxa,
                        data_dir / "aohs" / scenario / taxa,
                        data_dir / "aohs" / "pnv" / taxa,
                        curve,
                        data_dir / "deltap" / scenario / curve / taxa,
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
