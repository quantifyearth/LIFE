import argparse
import itertools
from pathlib import Path
from multiprocessing import set_start_method
from typing import Dict, List, Optional

import pandas as pd
import yirgacheffe.operators as yo # type: ignore
from alive_progress import alive_bar # type: ignore
from yirgacheffe.layers import RasterLayer # type: ignore

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


def make_current_map(
    jung_path: Path,
    update_masks_path: Optional[Path],
    crosswalk_path: Path,
    output_path: Path,
    concurrency: Optional[int],
    show_progress: bool,
) -> None:

    if update_masks_path is not None:
        update_masks = [
            RasterLayer.layer_from_file(x) for x in sorted(list(update_masks_path.glob("*.tif")))
        ]
    else:
        update_masks = []

    with RasterLayer.layer_from_file(jung_path) as jung:
        crosswalk = load_crosswalk_table(crosswalk_path)

        map_preserve_code = list(itertools.chain.from_iterable([crosswalk[x] for x in IUCN_CODE_ARTIFICAL]))

        updated_jung = jung
        for update in update_masks:
            updated_jung = yo.where(update != 0, update, updated_jung)

        current_map = yo.where(
            updated_jung.isin(map_preserve_code),
            updated_jung,
            (yo.floor(updated_jung / 100) * 100).astype(yo.DataType.UInt16),
        )

        with RasterLayer.empty_raster_layer_like(
            jung,
            filename=output_path,
            threads=16
        ) as result:
            if show_progress:
                with alive_bar(manual=True) as bar:
                    current_map.parallel_save(result, callback=bar, parallelism=concurrency)
            else:
                current_map.parallel_save(result, parallelism=concurrency)

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

    make_current_map(
        args.jung_path,
        args.update_masks_path,
        args.crosswalk_path,
        args.results_path,
        args.concurrency,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
