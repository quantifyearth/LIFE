import argparse
import itertools
import math
import os
from contextlib import ExitStack, nullcontext
from pathlib import Path

import pandas as pd
import psutil
import yirgacheffe as yg
from alive_progress import alive_bar


def load_crosswalk_table(table_file_name: Path) -> dict[str,list[int]]:
    rawdata = pd.read_csv(table_file_name)
    result: dict[str,list[int]] = {}
    for _, row in rawdata.iterrows():
        try:
            result[row.code].append(int(row.value))
        except KeyError:
            result[row.code] = [int(row.value)]
    return result

def make_restore_map(
    pnv_path: Path,
    current_dir_path: Path,
    crosswalk_path: Path,
    output_path: Path,
    iucn_codes: list[str],
    parallelism: int | None,
    show_progress: bool,
) -> None:
    os.makedirs(output_path, exist_ok=True)

    crosswalk = load_crosswalk_table(crosswalk_path)

    map_replacement_codes = list(itertools.chain.from_iterable([crosswalk[x] for x in iucn_codes]))
    ideal_map_replacement_filenames = [current_dir_path / f"lcc_{code}.tif" for code in map_replacement_codes]
    map_replacement_filenames = [path for path in ideal_map_replacement_filenames if path.exists()]

    with ExitStack() as stack:
        replacement_maps = [stack.enter_context(yg.read_raster(filename)) for filename in map_replacement_filenames]
        replacement_total = yg.sum(replacement_maps)

        # all the ones we expect to be left with, but not the ones we're removing
        current_raster_filenames = [
            path for path in current_dir_path.glob("*.tif") if path not in map_replacement_filenames
        ]

        # Read the PNV as the same scale as the other maps
        with yg.read_raster_like(pnv_path, replacement_maps[0], yg.ResamplingMethod.Nearest) as pnv:

            for filename in current_raster_filenames:
                lcc_code = int(filename.stem.split('_')[1])
                ctx = alive_bar(manual=True, title=str(lcc_code)) if show_progress else nullcontext()
                with ctx as bar:
                    with yg.read_raster(filename) as layer:

                        if parallelism is not None:
                            # If we use all the cores on bigger machines we'll run out of memory
                            # as Yirgacheffe isn't that smart yet unfortunately
                            mem = psutil.virtual_memory()
                            estimated_memory_per_row = (layer.window.xsize * 8) * 2
                            estimated_rows_per_free_memory = mem.free / estimated_memory_per_row
                            estimated_chunk_size = estimated_rows_per_free_memory / parallelism

                        updated_layer = layer + (replacement_total * (pnv == lcc_code).astype(yg.DataType.Float32))
                        capped_updated_layer = yg.where(updated_layer > 1, 1.0, updated_layer)

                        if parallelism is not None:
                            capped_updated_layer.ystep = min(math.floor(estimated_chunk_size), yg.constants.YSTEP)

                        capped_updated_layer.to_geotiff(
                            output_path / f"lcc_{lcc_code}.tif",
                            callback=bar,
                            parallelism=parallelism,
                        )

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate restore scenario counterfactual maps.")
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
        help='Path of current maps',
        required=True,
        dest='current_dir_path',
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
        help='Path where final maps should be stored',
        required=True,
        dest='results_path',
    )
    parser.add_argument(
        '--codes',
        type=str,
        help='Comma-separated IUCN habitat codes to restore (e.g. 14.1,14.2,14.3)',
        required=True,
        dest='codes',
    )
    parser.add_argument(
        '-j',
        type=int,
        help='Number of parallel threads to use for calculation.',
        required=False,
        default=None,
        dest='parallelism',
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
        args.current_dir_path,
        args.crosswalk_path,
        args.results_path,
        args.codes.split(','),
        args.parallelism,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
