import argparse
import os
from multiprocessing import cpu_count

import numpy as np
from osgeo import gdal
from yirgacheffe.layers import RasterLayer

layers = ["all", "AMPHIBIANS", "AVES", "MAMMALIA", "REPTILES"]
layers = ["AMPHIBIANS", "AVES", "MAMMALIA", "REPTILES"]


def binary_maps(
    map_path: str,
    output_path: str,
    parallelism: int
) -> None:
    output_dir, filename = os.path.split(output_path)
    base, _ext = os.path.splitext(filename)
    os.makedirs(output_dir, exist_ok=True)

    for index, layername in enumerate(layers):
        with RasterLayer.layer_from_file(map_path, band=index+1) as inputmap:
            with RasterLayer.empty_raster_layer_like(
                inputmap,
                filename=os.path.join(output_dir, f"{base}_{layername}_binary.tif"),
                datatype=gdal.GDT_Int16,
            ) as result:
                calc = inputmap.numpy_apply(lambda c: np.where(c != 0, c / np.abs(c), 0))
                calc.parallel_save(result, parallelism=parallelism)

def main() -> None:
    parser = argparse.ArgumentParser(description="Converts the output maps to binary")
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        dest="input_filename",
        help="multilayer result geotiff"
    )
    parser.add_argument(
        "--output",
        type=str,
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
