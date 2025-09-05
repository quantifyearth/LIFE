import argparse
import math
import os
import tempfile
from pathlib import Path

import yirgacheffe as yg
import yirgacheffe.operators as yo

from make_area_map import make_area_map

DISAGG_CUTOFF = yg.constant(0.95)

def build_gaez_hyde(
    gaez_path: Path,
    hyde_path: Path,
    output_dir_path: Path,
) -> None:
    os.makedirs(output_dir_path, exist_ok=True)

    with yg.read_raster(gaez_path) as gaez:
        with yg.read_raster(hyde_path) as hyde:
            assert gaez.map_projection == hyde.map_projection
            projection = gaez.map_projection

            with tempfile.TemporaryDirectory() as tmpdir:
                area_raster = Path(tmpdir) / "area.tif"
                make_area_map(math.fabs(projection.ystep), area_raster)

                with yg.read_narrow_raster(area_raster) as area:

                    portional_hyde = (hyde.nan_to_num() * 1000000) / area
                    portional_gaez = gaez / 100.0

                    # where gaez and hyde disagree (sum greater than disagg cutoff), scale down
                    uncapped_total = portional_gaez + portional_hyde
                    # NaNs stop warnings about divide by zero
                    uncapped_total_with_nan = yo.where(uncapped_total == 0.0, float("nan"), uncapped_total)

                    # calculate ag-perc scalars
                    total = yo.where(
                        uncapped_total_with_nan >= DISAGG_CUTOFF,
                        DISAGG_CUTOFF - (yg.constant(1) / yo.exp(uncapped_total_with_nan * 2)),
                        uncapped_total_with_nan,
                    )

                    gaez_ratio = portional_gaez / uncapped_total_with_nan
                    gaez_values = total * gaez_ratio
                    gaez_values.to_geotiff(output_dir_path / "crop.tif")

                    hyde_ratio = portional_hyde / uncapped_total_with_nan
                    hyde_values = total * hyde_ratio
                    hyde_values.to_geotiff(output_dir_path / "pasture.tif")

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a combined GAEZ and Hyde layers")
    parser.add_argument(
        '--gaez',
        type=Path,
        help='Gaez raster',
        required=True,
        dest='gaez_path',
    )
    parser.add_argument(
        '--hyde',
        type=Path,
        help='Hyde raster',
        required=True,
        dest='hyde_path',
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Path of directory for combined rasters raster',
        required=True,
        dest='output_dir_path',
    )
    args = parser.parse_args()

    build_gaez_hyde(
        args.gaez_path,
        args.hyde_path,
        args.output_dir_path,
    )

if __name__ == "__main__":
    main()
