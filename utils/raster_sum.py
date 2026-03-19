import argparse
import os
import resource
from pathlib import Path

import yirgacheffe as yg
from alive_progress import alive_bar # type: ignore

def raster_sum(
    images_dir: Path,
    output_filename: Path,
) -> None:
    # We'll be opening all the deltap files per taxa in one, so we'll need to raise
    # the number of files we can open.
    _, max_fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (max_fd_limit, max_fd_limit))

    layers = [yg.read_raster(x) for x in images_dir.glob("*.tif")]
    total = yg.sum(layers)
    with alive_bar(manual=True) as bar:
        total.to_geotiff(output_filename, callback=bar, parallelism=True)

def main() -> None:
    parser = argparse.ArgumentParser(description="Sums many rasters into a single raster")
    parser.add_argument(
        "--rasters_directory",
        type=Path,
        required=True,
        dest="rasters_directory",
        help="Folder containing all the deltap rasters for a given scenario in their respective subdirectories."
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        dest="output_filename",
        help="Destination geotiff file for results."
    )
    args = parser.parse_args()

    raster_sum(
        args.rasters_directory,
        args.output_filename,
    )

if __name__ == "__main__":
    main()
