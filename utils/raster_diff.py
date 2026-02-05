import argparse
import os
from pathlib import Path

import yirgacheffe as yg
from snakemake_argparse_bridge import snakemake_compatible # type: ignore

def raster_diff(
    raster_a_path: Path,
    raster_b_path: Path,
    output_path: Path,
) -> None:
    os.makedirs(output_path.parent, exist_ok=True)
    with (
        yg.read_raster(raster_a_path) as raster_a,
        yg.read_raster(raster_b_path) as raster_b
    ):
        result = raster_a - raster_b
        result.to_geotiff(output_path, parallelism=True)

@snakemake_compatible(mapping={
    "raster_a_path": "input.raster_a",
    "raster_b_path": "input.raster_b",
    "output_path": "output.diff",
})
def main() -> None:
    parser = argparse.ArgumentParser(description="Calculates the difference between two rasters")
    parser.add_argument(
        "--raster_a",
        type=Path,
        required=True,
        dest="raster_a_path",
        help="Left hand side of the difference."
    )
    parser.add_argument(
        "--raster_b",
        type=Path,
        required=True,
        dest="raster_b_path",
        help="Right hand side of the difference"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        dest="output_path",
        help="Destination geotiff file for results."
    )
    args = parser.parse_args()

    raster_diff(
        args.raster_a_path,
        args.raster_b_path,
        args.output_path,
    )

if __name__ == "__main__":
    main()
