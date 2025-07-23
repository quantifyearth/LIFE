import argparse
import itertools
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yirgacheffe.operators as yo
from alive_progress import alive_bar
from yirgacheffe.layers import RasterLayer, RescaledRasterLayer

from osgeo import gdal
gdal.SetCacheMax(1 * 1024 * 1024 * 1024)

# From Eyres et al: In the restoration scenario all areas classified as arable or pasture were restored to their PNV
IUCN_CODE_REPLACEMENTS = [
    "14.1",
    "14.2",
    "14.3",
    "14.4",
    "14.6"
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


def make_restore_map(
    pnv_path: Path,
    current_path: Path,
    crosswalk_path: Path,
    output_path: Path,
    concurrency: Optional[int],
    show_progress: bool,
) -> None:
    with RasterLayer.layer_from_file(current_path) as current:
        with RescaledRasterLayer.layer_from_file(pnv_path, current.pixel_scale) as pnv:
            crosswalk = load_crosswalk_table(crosswalk_path)

            map_replacement_codes = list(itertools.chain.from_iterable([crosswalk[x] for x in IUCN_CODE_REPLACEMENTS]))
            restore_map = yo.where(current.isin(map_replacement_codes), pnv, current)

            with RasterLayer.empty_raster_layer_like(
                restore_map,
                filename=output_path,
                threads=16
            ) as result:
                if show_progress:
                    with alive_bar(manual=True) as bar:
                        restore_map.parallel_save(result, callback=bar, parallelism=concurrency)
                else:
                    restore_map.parallel_save(result, parallelism=concurrency)


def main() -> None:
    parser = argparse.ArgumentParser(description="Zenodo resource downloader.")
    parser.add_argument(
        '--pnv',
        type=Path,
        help='Path of PNV map',
        required=True,
        dest='pnv_path',
    )
    parser.add_argument(
        '--current',
        type=Path,
        help='Path of current map',
        required=True,
        dest='current_path',
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

    make_restore_map(
        args.pnv_path,
        args.current_path,
        args.crosswalk_path,
        args.results_path,
        args.concurrency,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
