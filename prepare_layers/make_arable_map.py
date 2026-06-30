import argparse
import math
import os
import shutil
from contextlib import nullcontext
from pathlib import Path

import psutil
import yirgacheffe as yg
from alive_progress import alive_bar

JUNG_ARABLE_CODE = 1401
JUNG_URBAN_CODE = 1405

def make_arable_map(
    current_dir_path: Path,
    output_path: Path,
    parallelism: int | None,
    show_progress: bool,
) -> None:
    os.makedirs(output_path, exist_ok=True)

    # In this scenario all land that isn't urban is covered to arable
    urban_filename = current_dir_path / f"lcc_{JUNG_URBAN_CODE}.tif"
    new_arable_filename = output_path / f"lcc_{JUNG_ARABLE_CODE}.tif"

    shutil.copy(urban_filename, output_path)
    with yg.read_raster(urban_filename) as urban:

        new_arable = 1.0 - urban

        if parallelism is not None:
            # If we use all the cores on bigger machines we'll run out of memory
            # as Yirgacheffe isn't that smart yet unfortunately
            mem = psutil.virtual_memory()
            estimated_memory_per_row = (urban.window.xsize * 8) * 2
            estimated_rows_per_free_memory = mem.free / estimated_memory_per_row
            estimated_chunk_size = estimated_rows_per_free_memory / parallelism

            new_arable.ystep = min(math.floor(estimated_chunk_size), yg.constants.YSTEP)

        ctx = alive_bar(manual=True) if show_progress else nullcontext()
        with ctx as bar:
            new_arable.to_geotiff(new_arable_filename, callback=bar, parallelism=parallelism)

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
