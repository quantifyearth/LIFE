import argparse

import numpy as np
from yirgacheffe.layers import RasterLayer

SCALE = 1e6

def delta_p_scaled_area(
    input_path: str,
    diff_area_map_path: str,
    output_path: str,
):
    with RasterLayer.layer_from_file(diff_area_map_path) as area_restore:
        with RasterLayer.layer_from_file(input_path) as inlayer:

            intersection = RasterLayer.find_intersection([area_restore, inlayer])
            inlayer.set_window_for_intersection(intersection)
            area_restore.set_window_for_intersection(intersection)

            with RasterLayer.empty_raster_layer_like(inlayer, filename=output_path, nodata=float('nan')) as result:

                area_restore_filter = area_restore.numpy_apply(lambda c: np.where(c < SCALE, 0, c)) / SCALE
                filtered_layer = inlayer.numpy_apply(lambda il, af: np.where(af != 0, il, 0), area_restore_filter)
                scaled_filtered_layer = filtered_layer / area_restore_filter
                scaled_filtered_layer.save(result)

def main() -> None:
    parser = argparse.ArgumentParser(description="Scale final results.")
    parser.add_argument(
        '--input',
        type=str,
        help='Path of map of extinction risk',
        required=True,
        dest='current_path',
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