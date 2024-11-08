import argparse
import os
import sys
from glob import glob

import numpy as np
from yirgacheffe.layers import RasterLayer

SCALE = 1e6

def delta_p_scaled_area(
    input_path: str,
    diff_area_map_path: str,
    output_path: str,
):
    dirname, basename = os.path.split(output_path)
    os.makedirs(dirname, exist_ok=True)

    per_taxa = [
        RasterLayer.layer_from_file(os.path.join(input_path, x))
        for x in sorted(glob("*.tif", root_dir=input_path))
    ]
    if not per_taxa:
        sys.exit(f"Failed to find any per-taxa maps in {input_path}")

    area_restore = RasterLayer.layer_from_file(diff_area_map_path)

    for layer in per_taxa:
        try:
            layer.set_window_for_union(area_restore.area)
        except ValueError:
            layer.set_window_for_intersection(area_restore.area)

    area_restore_filter = area_restore.numpy_apply(lambda c: np.where(c < SCALE, float('nan'), c)) / SCALE

    per_taxa_path = os.path.join(dirname, f"per_taxa_{basename}")
    with RasterLayer.empty_raster_layer_like(
        area_restore,
        filename=per_taxa_path,
        nodata=float('nan'),
        bands=len(per_taxa)
    ) as result:
        for idx in range(len(per_taxa)):
            inlayer = per_taxa[idx]
            _, name = os.path.split(inlayer.name)
            result._dataset.GetRasterBand(idx+1).SetDescription(name[:-4])
            scaled_filtered_layer = inlayer.numpy_apply(
                lambda il, af: np.where(af != 0, (il / af) * -1.0, float('nan')),
                area_restore_filter
            )
            scaled_filtered_layer.parallel_save(result, band=idx + 1)

    summed_output_path = os.path.join(dirname, f"summed_{basename}")
    with RasterLayer.empty_raster_layer_like(area_restore, filename=summed_output_path, nodata=float('nan')) as result:
        summed_layer = per_taxa[0]
        for layer in per_taxa[1:]:
            summed_layer = summed_layer + layer
        scaled_filtered_layer = summed_layer.numpy_apply(
            lambda il, af: np.where(af != 0, (il / af) * -1.0, float('nan')),
            area_restore_filter
        )
        scaled_filtered_layer.parallel_save(result)

def main() -> None:
    parser = argparse.ArgumentParser(description="Scale final .")
    parser.add_argument(
        '--input',
        type=str,
        help='Path of map of extinction risk',
        required=True,
        dest='input_path',
    )
    parser.add_argument(
        '--diffmap',
        type=str,
        help='Path of map of scenario difference scaled by area',
        required=True,
        dest='diff_area_map_path',
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Path where final map should be stored',
        required=True,
        dest='output_path',
    )
    args = parser.parse_args()

    delta_p_scaled_area(
        args.input_path,
        args.diff_area_map_path,
        args.output_path
    )

if __name__ == "__main__":
    main()
