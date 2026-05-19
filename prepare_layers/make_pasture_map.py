import argparse
import os
import shutil
from contextlib import nullcontext
from pathlib import Path

import yirgacheffe as yg
from alive_progress import alive_bar

JUNG_PASTURE_CODE = 1402
JUNG_URBAN_CODE = 1405

def make_pasture_map(
    current_dir_path: Path,
    output_path: Path,
    parallelism: int | None,
    show_progress: bool,
) -> None:
    os.makedirs(output_path, exist_ok=True)

    # In this scenario all land that isn't urban is covered to arable
    urban_filename = current_dir_path / f"lcc_{JUNG_URBAN_CODE}.tif"
    new_pasture_filename = output_path / f"lcc_{JUNG_PASTURE_CODE}.tif"

    shutil.copy(urban_filename, output_path)
    with yg.read_raster(urban_filename) as urban:
        new_pasture = 1.0 - urban
        ctx = alive_bar(manual=True) if show_progress else nullcontext()
        with ctx as bar:
            new_pasture.to_geotiff(new_pasture_filename, callback=bar, parallelism=parallelism)

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the pasture scenario map.")
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

    make_pasture_map(
        args.current_path,
        args.results_path,
        args.concurrency,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
