import argparse
import os
from multiprocessing import cpu_count
from pathlib import Path

import yirgacheffe as yg

LAYERS = ["all", "AMPHIBIANS", "AVES", "MAMMALIA", "REPTILES"]
# LAYERS = ["AMPHIBIANS", "AVES", "MAMMALIA", "REPTILES"]

def binary_maps(
    map_path: Path,
    output_path: Path,
    parallelism: int
) -> None:
    os.makedirs(output_path.parent, exist_ok=True)

    for index, layername in enumerate(LAYERS):
        with yg.read_raster(map_path, band=index+1) as inputmap:
            binary_map = yg.where(inputmap != 0, inputmap / yg.abs(inputmap), 0)
            binary_map.astype(yg.DataType.Int16).to_geotiff(
                output_path.parent / f"{output_path.stem}_{layername}_binary.tif",
                parallelism=parallelism,
            )

def main() -> None:
    parser = argparse.ArgumentParser(description="Converts the output maps to binary")
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        dest="input_filename",
        help="multilayer result geotiff"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        dest="output_filename",
        help="Destination geotiff path."
    )
    parser.add_argument(
        "-j",
        type=int,
        required=False,
        default=round(cpu_count() / 2),
        dest="processes_count",
        help="Number of parallel threads to use."
    )
    args = parser.parse_args()

    binary_maps(
        args.input_filename,
        args.output_filename,
        args.processes_count
    )

if __name__ == "__main__":
    main()
