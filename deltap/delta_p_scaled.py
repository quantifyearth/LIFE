import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import yirgacheffe.operators as yo
from yirgacheffe.layers import RasterLayer

SCALE = 1e6

def delta_p_scaled_area(
    input_path: Path,
    diff_area_map_path: Path,
    totals_path: Path,
    output_path: Path,
):
    os.makedirs(output_path.parent, exist_ok=True)

    per_taxa = [
        RasterLayer.layer_from_file(os.path.join(input_path, x))
        for x in sorted(input_path.glob("*.tif"))
    ]
    if not per_taxa:
        sys.exit(f"Failed to find any per-taxa maps in {input_path}")

    area_restore = RasterLayer.layer_from_file(diff_area_map_path)

    total_counts = pd.read_csv(totals_path)

    area_restore_filter = yo.where(area_restore < SCALE, float('nan'), area_restore) / SCALE

    with RasterLayer.empty_raster_layer_like(
        area_restore,
        filename=output_path,
        nodata=float('nan'),
        bands=len(per_taxa) + 1
    ) as result:

        species_count = int(total_counts[total_counts.taxa=="all"]["count"].values[0])

        result._dataset.GetRasterBand(1).SetDescription("all")  # pylint: disable=W0212
        summed_layer = per_taxa[0]
        for layer in per_taxa[1:]:
            summed_layer = summed_layer + layer

        scaled_filtered_layer = yo.where(
            area_restore_filter != 0,
            ((summed_layer / area_restore_filter) * -1.0) / species_count,
            float('nan')
        )
        scaled_filtered_layer.parallel_save(result, band=1)

        for idx, inlayer in enumerate(per_taxa):
            assert inlayer.name
            _, name = os.path.split(inlayer.name)
            taxa = name[:-4]
            species_count = int(total_counts[total_counts.taxa==taxa]["count"].values[0])
            result._dataset.GetRasterBand(idx + 2).SetDescription(taxa)  # pylint: disable=W0212
            scaled_filtered_layer = yo.where(
                area_restore_filter != 0,
                ((inlayer / area_restore_filter) * -1.0) / species_count,
                float('nan')
            )
            scaled_filtered_layer.parallel_save(result, band=idx + 2)


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
