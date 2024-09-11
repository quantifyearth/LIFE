import argparse 
from typing import Dict, List, Optional

from osgeo import gdal
from alive_progress import alive_bar
from yirgacheffe.layers import RasterLayer

def make_arable_map(
    current_path: str,
    scenario_path: str,
    output_path: str,
    concurrency: Optional[int],
    show_progress: bool,
) -> None:
    with RasterLayer.layer_from_file(current_path) as current:
        with RasterLayer.layer_from_file(scenario_path) as scenario:

            layers = [current, scenario]
            intersection = RasterLayer.find_intersection(layers)
            for layer in layers:
                layer.set_window_for_intersection(intersection)

            calc = current.numpy_apply(lambda a, b: a != b)

            with RasterLayer.empty_raster_layer_like(
                current,
                filename=output_path,
                datatype=gdal.GDT_Float32,
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
        '--scenario',
        type=str,
        help='Path of the scenario map',
        required=True,
        dest='scenario_path',
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
        args.scenario_path,
        args.results_path,
        args.concurrency,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
