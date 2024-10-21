import argparse 
import itertools
import os
import shutil
import tempfile
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from alive_progress import alive_bar
from yirgacheffe.layers import RasterLayer

# From Eyres et al:
# All natural terrestrial habitats and non-urban artificial habitats
IUCN_CODE_NATURAL = [
    "1", "1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7", "1.8", "1.9",
    "2", "2.1", "2.2",
    "3", "3.1", "3.2", "3.3", "3.4", "3.5", "3.6", "3.7", "3.8",
    "4", "4.1", "4.2", "4.3", "4.4", "4.5", "4.6", "4.7",
    "6",
    "8", "8.1", "8.2", "8.3",
    "14.1", "14.2", "14.3", "14.4", "14.6", # urban (14.5) removed
    #"16", # Not in crosswalk due to iucn_modlib
    "17",
    #"18", # Not in crosswalk due to iucn_modlib
]
ARABLE = "14.1"

def load_crosswalk_table(table_file_name: str) -> Dict[str,int]:
    rawdata = pd.read_csv(table_file_name)
    result = {}
    for _, row in rawdata.iterrows():
        try:
            result[row.code].append(int(row.value))
        except KeyError:
            result[row.code] = [int(row.value)]
    return result


def make_arable_map(
    current_path: str,
    crosswalk_path: str,
    output_path: str,
    concurrency: Optional[int],
    show_progress: bool,
) -> None:
    with RasterLayer.layer_from_file(current_path) as current:
        crosswalk = load_crosswalk_table(crosswalk_path)

        map_replace_codes = list(set(list(itertools.chain.from_iterable([crosswalk[x] for x in IUCN_CODE_NATURAL]))))
        print(map_replace_codes)
        # arable_code = crosswalk[ARABLE][0]
        arable_code = 1401 # This is a hack as Daniele's crosswalk has 14.1 mapped to both 1400 and 1401 and there's no logical way
        # to understand this

        calc = current.numpy_apply(
            lambda a: np.where(np.isin(a, map_replace_codes), arable_code, a)
        )

        with RasterLayer.empty_raster_layer_like(
            current,
            filename=output_path,
            threads=16
        ) as result:
            if show_progress:
                with alive_bar(manual=True) as bar:
                    calc.parallel_save(result, callback=bar, parallelism=concurrency)
            else:
                calc.parallel_save(result, parallelism=concurrency)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the arable scenario map.")
    parser.add_argument(
        '--current',
        type=str,
        help='Path of Jung L2 map',
        required=True,
        dest='current_path',
    )
    parser.add_argument(
        '--crosswalk',
        type=str,
        help='Path of map to IUCN crosswalk table',
        required=True,
        dest='crosswalk_path',
    )
    parser.add_argument(
        '--output',
        type=str,
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
        args.current_path,
        args.crosswalk_path,
        args.results_path,
        args.concurrency,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
