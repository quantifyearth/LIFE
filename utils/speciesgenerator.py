import argparse
import sys
from pathlib import Path
from typing import List

import pandas as pd

DEFAULT_SCENARIOS = ["current", "pnv"]

def species_generator(
    data_dir: Path,
    output_csv_path: Path,
    scenarios: List[str],
    habitats_path: Path | None,
    aohs_path: Path | None,
):
    species_info_dir = data_dir / "species-info"
    taxas = [x.name for x in species_info_dir.iterdir()]

    if habitats_path is None:
        habitats_path = data_dir / "habitat_maps"

    if aohs_path is None:
        aohs_path = data_dir / "aohs"

    res = []
    for taxa in taxas:
        for scenario in scenarios:
            habitat_maps_path = habitats_path / scenario
            if not habitat_maps_path.exists():
                sys.exit(f"Expected to find habitat maps in {habitat_maps_path}")

            source = 'historic' if scenario == 'pnv' else 'current'
            taxa_path = species_info_dir / taxa / source
            if not taxa_path.exists():
                sys.exit(f"Expected to find list of species in {taxa_path}")

            speciess = taxa_path.glob("*.geojson")
            for species in speciess:
                res.append([
                    habitat_maps_path,
                    data_dir / "elevation-max.tif",
                    data_dir / "elevation-min.tif",
                    data_dir / "area-per-pixel.tif",
                    data_dir / "crosswalk.csv",
                    species,
                    aohs_path / scenario / taxa,
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
    parser.add_argument(
        '--scenarios',
        nargs='*',
        type=str,
        help="list of scenarios to calculate LIFE for",
        required=True,
        dest="scenarios",
    )
    parser.add_argument(
        '--habitats_path',
        type=Path,
        help="Path to directory containing different processed habitat layers",
        required=False,
        dest="habitats_path",
    )
    parser.add_argument(
        '--aohs_path',
        type=Path,
        help="Path to store AOHs in",
        required=False,
        dest="aohs_path",
    )
    args = parser.parse_args()

    species_generator(
        args.data_dir,
        args.output,
        args.scenarios + DEFAULT_SCENARIOS,
        args.habitats_path,
        args.aohs_path
    )

if __name__ == "__main__":
    main()
