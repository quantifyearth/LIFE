import argparse
from pathlib import Path
from typing import Optional

import yirgacheffe as yg
from alive_progress import alive_bar

from osgeo import gdal
gdal.SetCacheMax(1 * 1024 * 1024 * 1024)

JUNG_ARABLE_CODE = 1401
JUNG_URBAN_CODE = 1405

def make_arable_map(
    current_path: Path,
    output_path: Path,
    concurrency: Optional[int],
    show_progress: bool,
) -> None:
    with yg.read_raster(current_path) as current:
        arable_map = yg.where(current != JUNG_URBAN_CODE, JUNG_ARABLE_CODE, JUNG_URBAN_CODE)
        if show_progress:
            with alive_bar(manual=True) as bar:
                arable_map.to_geotiff(output_path, callback=bar, parallelism=concurrency)
        else:
            arable_map.to_geotiff(output_path, parallelism=concurrency)

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the arable scenario map.")
    parser.add_argument(
        '--current',
        type=Path,
        help='Path of current map',
        required=True,
        dest='current_path',
    )
    parser.add_argument(
        '--output',
        type=Path,
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
