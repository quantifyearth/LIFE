import argparse
import os
from pathlib import Path

import pandas as pd
import yirgacheffe as yg
from snakemake_argparse_bridge import snakemake_compatible  # type: ignore

def sum_dir(directory: Path) -> dict[int,float]:
    result: dict[int,float] = {}
    for raster_path in directory.glob("lcc_*.tif"):
        class_num = int(raster_path.stem.split("_")[1])
        with (
            yg.read_raster(raster_path) as raster,
            yg.area_raster(raster.map_projection) as area_raster,
        ):
            result[class_num] = (raster * area_raster).parallel_sum()
    return result

def land_cover_area(
    jung_current_dir: Path,
    current_dir: Path,
    output_filename: Path,
) -> None:
    os.makedirs(output_filename.parent, exist_ok=True)

    jung_current_areas = sum_dir(jung_current_dir)
    current_areas = sum_dir(current_dir)

    all_classes = sorted(set(jung_current_areas) | set(current_areas))
    rows = [
        [cls, jung_current_areas.get(cls, 0.0), current_areas.get(cls, 0.0)]
        for cls in all_classes
    ]

    df = pd.DataFrame(rows, columns=["land_cover_class", "jung current", "hybrid current"])
    df.to_csv(output_filename, index=False)


@snakemake_compatible(mapping={
    "jung_current_dir": "params.jung_current_dir",
    "current_dir": "params.current_dir",
    "output_filename": "output[0]",
})
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare land cover areas between jung_current and current maps."
    )
    parser.add_argument(
        "--jung-current",
        type=Path,
        required=True,
        dest="jung_current_dir",
        help="Directory of jung_current lcc_*.tif rasters",
    )
    parser.add_argument(
        "--current",
        type=Path,
        required=True,
        dest="current_dir",
        help="Directory of current lcc_*.tif rasters",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        dest="output_filename",
        help="Destination CSV path",
    )
    args = parser.parse_args()

    land_cover_area(
        args.jung_current_dir,
        args.current_dir,
        args.output_filename,
    )


if __name__ == "__main__":
    main()
