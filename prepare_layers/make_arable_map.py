import argparse
import os
import shutil
from pathlib import Path
from typing import Optional

import yirgacheffe as yg
from alive_progress import alive_bar

JUNG_ARABLE_CODE = 1401
JUNG_URBAN_CODE = 1405

def make_arable_map(
    current_dir_path: Path,
    output_path: Path,
    concurrency: Optional[int],
    show_progress: bool,
) -> None:
    os.makedirs(output_path, exist_ok=True)

    # In this scenario all land that isn't urban is covered to arable
    urban_filename = current_dir_path / f"lcc_{JUNG_URBAN_CODE}.tif"
    new_arable_filename = output_path / f"lcc_{JUNG_ARABLE_CODE}.tif"

    shutil.copy(urban_filename, output_path)
    with yg.read_raster(urban_filename) as urban:
        new_arable = 1.0 - urban
        if show_progress:
            with alive_bar(manual=True) as bar:
                new_arable.to_geotiff(new_arable_filename, callback=bar, parallelism=concurrency)
        else:
            new_arable.to_geotiff(new_arable_filename, parallelism=concurrency)

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the arable scenario map.")
    parser.add_argument(
        '--current',
        type=Path,
        help='Path of fractional current maps',
        required=True,
        dest='current_dir_path',
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
        args.current_dir_path,
        args.results_path,
        args.concurrency,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
