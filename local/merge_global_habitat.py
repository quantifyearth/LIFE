"""
This script is used to merge localised raster data into a global raster map.

Specifically it was used originally to merge crosswalked Brazil Mapbiomass data
into the Jung habitat map.
"""

import argparse
from contextlib import nullcontext
from pathlib import Path

import yirgacheffe as yg
from alive_progress import alive_bar # type: ignore

def merge_global_habitat(
    global_layer_path: Path,
    local_layer_path: Path,
    output_layer_path: Path,
    show_progress: bool,
) -> None:
    # Note, we assume naively the local data is higher resolution than the global layer for now
    # In a better world we'd work out which is higher res and make everything in that pixel scale
    with (
        yg.read_raster(local_layer_path) as local_layer,
        yg.read_raster_like(global_layer_path, local_layer, yg.ResamplingMethod.Nearest) as global_layer,
    ):
        local_layer.set_window_for_union(global_layer.area)
        cleared = local_layer.nan_to_num()
        combined = yg.where(cleared != 0, local_layer, global_layer)
        ctx = alive_bar(manual=True) if show_progress else nullcontext()
        with ctx as bar:
            combined.to_geotiff(output_layer_path, callback=bar, parallelism=True)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--global',
        type=Path,
        help="Global habitat raster",
        required=True,
        dest="global_layer_path"
    )
    parser.add_argument(
        '--local',
        type=Path,
        required=True,
        dest="local_layer_path",
        help="Local habitat raster",
    )
    parser.add_argument(
        '--output',
        type=Path,
        required=True,
        dest="output_layer_path",
        help="Result combined raster path",
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

    merge_global_habitat(
        args.global_layer_path,
        args.local_layer_path,
        args.output_layer_path,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
