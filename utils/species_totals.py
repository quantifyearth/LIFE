import argparse
from pathlib import Path
from typing import Dict

import pandas as pd

TAXA = ["AMPHIBIA", "AVES", "MAMMALIA", "REPTILIA"]

def species_totals(
    deltaps_path: Path,
    output_path: Path,
) -> None:
    res : Dict[str,int] = {}
    for taxa in TAXA:
        taxa_path = deltaps_path / taxa
        count = len(list(taxa_path.glob("*.tif")))
        res[taxa] = count

    df = pd.DataFrame([[a, b] for a, b in res.items()], columns=["taxa", "count"])
    df.loc[-1] = ["all", df["count"].sum()]
    df.to_csv(output_path, index=False)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--deltaps",
        type=Path,
        required=True,
        dest="deltaps_path",
        help="Per species deltap folder"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        dest="output_filename",
        help="Destination CSV path."
    )
    args = parser.parse_args()

    species_totals(
        args.deltaps_path,
        args.output_filename,
    )

if __name__ == "__main__":
    main()
