import argparse
from typing import Optional

import numpy as np
from alive_progress import alive_bar
from yirgacheffe.layers import RasterLayer

from osgeo import gdal
gdal.SetCacheMax(1 * 1024 * 1024 * 1024)

JUNG_ARABLE_CODE = 1401
JUNG_URBAN_CODE = 1405

def make_arable_map(
    current_path: str,
    output_path: str,
    concurrency: Optional[int],
    show_progress: bool,
) -> None:
    with RasterLayer.layer_from_file(current_path) as current:

        calc = current.numpy_apply(
            lambda a: np.where(a != JUNG_URBAN_CODE, JUNG_ARABLE_CODE, a)
        )

        with RasterLayer.empty_raster_layer_like(
            current,
            filename=output_path,
            threads=16
        ) as result:
            if show_progress:
                with alive_bar(manual=True) as bar:
                    calc.parallel_save(result, callback=bar, parallelism=concurrency)
            else:
                calc.parallel_save(result, parallelism=concurrency)

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the arable scenario map.")
    parser.add_argument(
        '--current',
        type=str,
        help='Path of Jung L2 map',
        required=True,
        dest='current_path',
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Path where final map should be stored',
        required=True,
        dest='results_path',
    )
    parser.add_argument(
        '-j',
        type=int,
        help='Number of concurrent threads to use for calculation.',
        required=False,
        default=None,
        dest='concurrency',
    )
    parser.add_argument(
        '-p',
        help="Show progress indicator",
        default=False,
        required=False,
        action='store_true',
        dest='show_progress',
    )
    args = parser.parse_args()

    make_arable_map(
        args.current_path,
        args.results_path,
        args.concurrency,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
