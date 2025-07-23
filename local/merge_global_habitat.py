import argparse
from pathlib import Path

from yirgacheffe.layers import RasterLayer, RescaledRasterLayer
import yirgacheffe.operators as yo

from osgeo import gdal
gdal.SetCacheMax(32 * 1024 * 1024)

def merge_global_habitat(
    global_layer_path: Path,
    local_layer_path: Path,
    output_layer_path: Path,
) -> None:
    # Note, we assume naively the local data is higher resolution than the global layer for now
    with RasterLayer.layer_from_file(local_layer_path) as local_layer:
        with RescaledRasterLayer.layer_from_file(
            global_layer_path,
            pixel_scale=local_layer.pixel_scale
        ) as global_layer:
            local_layer.set_window_for_union(global_layer.area)
            cleared = local_layer.nan_to_num()
            combined = yo.where(cleared != 0, local_layer, global_layer)
            with RasterLayer.empty_raster_layer_like(
                combined,
                filename=output_layer_path,
                datatype=local_layer.datatype
            ) as result:
                combined.parallel_save(result, parallelism=200)

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
    args = parser.parse_args()

    merge_global_habitat(
        args.global_layer_path,
        args.local_layer_path,
        args.output_layer_path,
    )

if __name__ == "__main__":
    main()
