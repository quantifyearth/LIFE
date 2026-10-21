import argparse
import shutil
import tempfile
from contextlib import nullcontext
from pathlib import Path
from typing import Optional

import pandas as pd
from alive_progress import alive_bar
from osgeo import gdal
import yirgacheffe as yg

gdal.SetCacheMax(512 * 1024 * 1024)

def make_diff_map(
    current_path: Path,
    habitat_code: str,
    crosswalk_path: Path,
    pixel_scale: float,
    target_projection: Optional[str],
    output_path: Path,
    parallelism: Optional[int],
    show_progress: bool,
) -> None:
    crosswalk = pd.read_csv(crosswalk_path)
    translations = crosswalk[crosswalk.code==habitat_code]
    # The below is a horrible hack to get 1405 over 1400...
    specific_jung_code = list(translations.value)[-1]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        raw_map_filename = tmpdir_path / "raw.tif"
        print("comparing:")
        with yg.read_raster(current_path) as current:
            diff_map = current != specific_jung_code

            gdal.SetCacheMax(512 * 1024 * 1024)

            ctx = alive_bar(manual=True) if show_progress else nullcontext()
            with ctx as bar:
                diff_map.as_type(yg.DataType.Float32).to_geotiff(
                    raw_map_filename, callback=bar, parallelism=parallelism
                )

        gdal.SetCacheMax(256 * 1024 * 1024 * 1024)
        rescaled_map_filename = tmpdir_path /  "rescaled.tif"
        print("reprojecting:")
        with alive_bar(manual=True) as bar:
            gdal.Warp(rescaled_map_filename, raw_map_filename, options=gdal.WarpOptions(
                creationOptions=['COMPRESS=LZW', 'NUM_THREADS=16'],
                multithread=True,
                dstSRS=target_projection,
                outputType=gdal.GDT_Float32,
                xRes=pixel_scale,
                yRes=0.0 - pixel_scale,
                resampleAlg="average",
                workingType=gdal.GDT_Float32,
                callback=lambda a, _b, _c: bar(a), # pylint: disable=E1102
            ))

        print("scaling result:")
        with (
            yg.read_raster(rescaled_map_filename) as diff_map,
            yg.area_raster(diff_map.projection) as area_map,
        ):
            area_adjusted_map_filename = tmpdir_path /  "final.tif"
            final = area_map * diff_map
            gdal.SetCacheMax(512 * 1024 * 1024)

            ctx = alive_bar(manual=True) if show_progress else nullcontext()
            with ctx as bar:
                final.as_type(yg.DataType.Float32).to_geotiff(
                    area_adjusted_map_filename, callback=bar, parallelism=parallelism
                )

            shutil.move(area_adjusted_map_filename, output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an area difference map.")
    parser.add_argument(
        '--current',
        type=Path,
        help='Path of current map',
        required=True,
        dest='current_path',
    )
    parser.add_argument(
        '--habitat_code',
        type=str,
        help='IUCN habitat code',
        required=True,
        dest='habitat_code',
    )
    parser.add_argument(
        '--crosswalk',
        type=Path,
        help='Path of map to IUCN crosswalk table',
        required=True,
        dest='crosswalk_path',
    )
    parser.add_argument(
        "--scale",
        type=float,
        required=True,
        dest="pixel_scale",
        help="Output pixel scale value."
    )
    parser.add_argument(
        '--projection',
        type=str,
        help="Target projection",
        required=False,
        dest="target_projection",
        default=None
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

    make_diff_map(
        args.current_path,
        args.habitat_code,
        args.crosswalk_path,
        args.pixel_scale,
        args.target_projection,
        args.results_path,
        args.concurrency,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
