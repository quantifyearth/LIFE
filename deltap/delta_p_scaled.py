import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import yirgacheffe as yg
from snakemake_argparse_bridge import snakemake_compatible # type: ignore

SCALE = 1e6

def delta_p_scaled_area(
    input_path: Path,
    diff_area_map_path: Path,
    species_totals_path: Path,
    output_path: Path,
):
    os.makedirs(output_path.parent, exist_ok=True)

    per_taxa = [
        yg.read_raster(x) for x in sorted(input_path.glob("*.tif"))
    ]
    if not per_taxa:
        sys.exit(f"Failed to find any per-taxa maps in {input_path}")

    species_total_counts = pd.read_csv(species_totals_path)

    diff_area = yg.read_raster(diff_area_map_path)
    diff_area_rescaled = yg.where(diff_area < SCALE, float('nan'), diff_area / SCALE)

    # Process all species in total
    total_species_count = int(species_total_counts[species_total_counts.taxa=="all"]["count"].values[0])
    summed_layer = yg.sum(per_taxa)
    all_scaled_filtered_layer = yg.where(
        diff_area_rescaled != 0,
        ((summed_layer / diff_area_rescaled) * -1.0) / total_species_count,
        float('nan')
    )

    bands = [all_scaled_filtered_layer]
    labels = ["all"]

    # Now per taxa
    for inlayer in per_taxa:
        # get the taxa from the filename
        _, name = os.path.split(inlayer.name)
        taxa = name[:-4]
        labels.append(taxa)

        taxa_species_count = int(species_total_counts[species_total_counts.taxa==taxa]["count"].values[0])
        scaled_filtered_layer = yg.where(
            diff_area_rescaled != 0,
            ((inlayer / diff_area_rescaled) * -1.0) / taxa_species_count,
            float('nan')
        )
        bands.append(scaled_filtered_layer)

    yg.to_geotiff(output_path, bands, labels, nodata=float('nan'))

@snakemake_compatible(mapping={
    "input_path": "params.input_dir",
    "diff_area_map_path": "input.diffmap",
    "totals_path": "input.totals",
    "output_path": "output.final",
})
def main() -> None:
    parser = argparse.ArgumentParser(description="Scale final results for publication.")
    parser.add_argument(
        '--input',
        type=Path,
        help='Path of map of extinction risk',
        required=True,
        dest='input_path',
    )
    parser.add_argument(
        '--diffmap',
        type=Path,
        help='Path of map of scenario difference scaled by area',
        required=True,
        dest='diff_area_map_path',
    )
    parser.add_argument(
        '--totals',
        type=Path,
        help='Path of CSV with total counts of spcies used',
        required=True,
        dest='totals_path',
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Path where final map should be stored',
        required=True,
        dest='output_path',
    )
    args = parser.parse_args()

    delta_p_scaled_area(
        args.input_path,
        args.diff_area_map_path,
        args.totals_path,
        args.output_path
    )

if __name__ == "__main__":
    main()
