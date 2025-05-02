import argparse
import os
import shutil
import tempfile
from typing import Optional

from osgeo import gdal
from alive_progress import alive_bar
from yirgacheffe.layers import RasterLayer, UniformAreaLayer

gdal.SetCacheMax(512 * 1024 * 1024)

def make_diff_map(
    current_path: str,
    scenario_path: str,
    area_path: str,
    pixel_scale: float,
    target_projection: Optional[str],
    output_path: str,
    concurrency: Optional[int],
    show_progress: bool,
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        raw_map_filename = os.path.join(tmpdir, "raw.tif")
        print("comparing:")
        with RasterLayer.layer_from_file(current_path) as current:
            with RasterLayer.layer_from_file(scenario_path) as scenario:

                layers = [current, scenario]
                intersection = RasterLayer.find_intersection(layers)
                for layer in layers:
                    layer.set_window_for_intersection(intersection)

                calc = current.numpy_apply(lambda a, b: a != b, scenario)

                gdal.SetCacheMax(512 * 1024 * 1024)
                with RasterLayer.empty_raster_layer_like(
                    current,
                    filename=raw_map_filename,
                    datatype=gdal.GDT_Float32,
                    threads=16
                ) as result:
                    if show_progress:
                        with alive_bar(manual=True) as bar:
                            calc.parallel_save(result, callback=bar, parallelism=concurrency)
                    else:
                        calc.parallel_save(result, parallelism=concurrency)

        gdal.SetCacheMax(256 * 1024 * 1024 * 1024)
        rescaled_map_filename = os.path.join(tmpdir, "rescaled.tif")
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
        with UniformAreaLayer.layer_from_file(area_path) as area_map:
            with RasterLayer.layer_from_file(rescaled_map_filename) as diff_map:
                layers = [area_map, diff_map]
                intersection = RasterLayer.find_intersection(layers)
                for layer in layers:
                    layer.set_window_for_intersection(intersection)

                area_adjusted_map_filename = os.path.join(tmpdir, "final.tif")
                calc = area_map * diff_map
                gdal.SetCacheMax(512 * 1024 * 1024)

                with RasterLayer.empty_raster_layer_like(
                    diff_map,
                    filename=area_adjusted_map_filename,
                    datatype=gdal.GDT_Float32,
                    threads=16
                ) as result:
                    if show_progress:
                        with alive_bar(manual=True) as bar:
                            calc.parallel_save(result, callback=bar, parallelism=concurrency)
                    else:
                        calc.parallel_save(result, parallelism=concurrency)

                shutil.move(area_adjusted_map_filename, output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an area difference map.")
    parser.add_argument(
        '--current',
        type=str,
        help='Path of Jung L2 map',
        required=True,
        dest='current_path',
    )
    parser.add_argument(
        '--scenario',
        type=str,
        help='Path of the scenario map',
        required=True,
        dest='scenario_path',
    )
    parser.add_argument(
        '--area',
        type=str,
        help='Path of the area per pixel map',
        required=True,
        dest='area_path',
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

    make_diff_map(
        args.current_path,
        args.scenario_path,
        args.area_path,
        args.pixel_scale,
        args.target_projection,
        args.results_path,
        args.concurrency,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
