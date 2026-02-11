import argparse
import itertools
import os
from pathlib import Path
from multiprocessing import set_start_method
from typing import Dict, List, Optional

import pandas as pd
import yirgacheffe as yg
from alive_progress import alive_bar # type: ignore
from snakemake_argparse_bridge import snakemake_compatible # type: ignore

from osgeo import gdal # type: ignore
gdal.SetCacheMax(1 * 1024 * 1024 * 1024)

# From Eyres et al: The current layer maps IUCN level 1 and 2 habitats, but habitats in the PNV layer are mapped
# only at IUCN level 1, so to estimate species’ proportion of original AOH now remaining we could only use natural
# habitats mapped at level 1 and artificial habitats at level 2.
IUCN_CODE_ARTIFICAL = [
    "14", "14.1", "14.2", "14.3", "14.4", "14.5", "14.6"
]

def load_crosswalk_table(table_file_name: Path) -> Dict[str,List[int]]:
    rawdata = pd.read_csv(table_file_name)
    result: Dict[str,List[int]] = {}
    for _, row in rawdata.iterrows():
        try:
            result[row.code].append(int(row.value))
        except KeyError:
            result[row.code] = [int(row.value)]
    return result

def make_current_maps(
    jung_path: Path,
    update_masks_path: Optional[Path],
    crosswalk_path: Path,
    output_dir_path: Path,
    concurrency: Optional[int],
    show_progress: bool,
    sentinel_path: Path | None,
) -> None:
    os.makedirs(output_dir_path, exist_ok=True)
    print(f"Using {concurrency} workers")

    if update_masks_path is not None:
        update_masks = [
            yg.read_raster(x) for x in sorted(list(update_masks_path.glob("*.tif")))
        ]
    else:
        update_masks = []

    with yg.read_raster(jung_path) as jung:
        crosswalk = load_crosswalk_table(crosswalk_path)

        map_preserve_code = list(itertools.chain.from_iterable([crosswalk[x] for x in IUCN_CODE_ARTIFICAL]))

        updated_jung = jung
        for update in update_masks:
            updated_jung = yg.where(update != 0, update, updated_jung)

        current_map = yg.where(
            updated_jung.isin(map_preserve_code),
            updated_jung,
            (yg.floor(updated_jung / 100) * 100),
        )

        print("Calculating unique land cover types...")
        vals = current_map.unique()

        for lcc in vals:
            print(f"Processing {lcc}...")
            per_class = current_map == lcc
            cast_per_class = per_class.astype(yg.DataType.Float32)
            with alive_bar(manual=True) as bar:
                cast_per_class.to_geotiff(
                    output_dir_path / f"lcc_{int(lcc)}.tif",
                    callback=bar,
                    parallelism=concurrency,
                    nodata=0.0,
                    sparse=True,
                )

    # This script generates a bunch of rasters, but snakemake needs one
    # output to say when this is done, so if we're in snakemake mode we touch a sentinel file to
    # let it know we've done. One day this should be another decorator.
    if sentinel_path is not None:
        os.makedirs(sentinel_path.parent, exist_ok=True)
        sentinel_path.touch()

@snakemake_compatible(mapping={
    "jung_path": "input.habitat",
    "update_masks_path": "params.updates_dir",
    "crosswalk_path": "input.crosswalk",
    "concurrency": "threads",
    "output_dir_path": "params.output_dir",
    "sentinel_path": "output.sentinel",
})
def main() -> None:
    set_start_method("spawn")

    parser = argparse.ArgumentParser(description="Generate the Level 1 current map")
    parser.add_argument(
        '--jung_l2',
        type=Path,
        help='Path of Jung L2 map',
        required=True,
        dest='jung_path',
    )
    parser.add_argument(
        '--update_masks',
        type=Path,
        help='Path of Jung L2 map update masks',
        required=False,
        dest='update_masks_path',
    )
    parser.add_argument(
        '--crosswalk',
        type=Path,
        help='Path of map to IUCN crosswalk table',
        required=True,
        dest='crosswalk_path',
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Path where final map should be stored',
        required=True,
        dest='output_dir_path',
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
    parser.add_argument(
        '--sentinel',
        type=Path,
        help='Generate a sentinel file on completion for snakemake to track',
        required=False,
        default=None,
        dest='sentinel_path',
    )
    args = parser.parse_args()

    make_current_maps(
        args.jung_path,
        args.update_masks_path,
        args.crosswalk_path,
        args.output_dir_path,
        args.concurrency,
        args.show_progress,
        args.sentinel_path,
    )

if __name__ == "__main__":
    main()
