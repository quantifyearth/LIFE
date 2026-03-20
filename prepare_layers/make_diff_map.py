import argparse
from contextlib import nullcontext
from pathlib import Path

import yirgacheffe as yg
from alive_progress import alive_bar # type: ignore
from snakemake_argparse_bridge import snakemake_compatible # type: ignore

POSSIBLE_HABITAT_CLASSES = [100, 200, 300, 400, 500, 600, 700, 800, 900,
    1000, 1100, 1200, 1300, 1400, 1401, 1402, 1403, 1404,
    1405, 1406, 1500, 1600, 1800]

def make_diff_map(
    current_path: Path,
    scenario_path: Path,
    output_path: Path,
    parallelism: None | int,
    show_progress: bool,
) -> None:
    layers = []
    for habitat in POSSIBLE_HABITAT_CLASSES:
        current_habitat_filename = current_path / f"lcc_{habitat}.tif"
        scenario_habitat_filename = scenario_path / f"lcc_{habitat}.tif"

        if not current_habitat_filename.exists() and not scenario_habitat_filename.exists():
            continue
        current_layer = yg.read_raster(current_habitat_filename) if current_habitat_filename.exists() \
            else yg.constant(0.0)
        scenario_layer = yg.read_raster(scenario_habitat_filename) if scenario_habitat_filename.exists() \
            else yg.constant(0.0)
        habitat_diff = current_layer != scenario_layer
        layers.append(habitat_diff)

    diff = yg.any(layers)
    area = yg.area_raster(diff.map_projection)
    scaled_diff = diff * area

    ctx = alive_bar(manual=True) if show_progress else nullcontext()
    with ctx as bar:
        scaled_diff.to_geotiff(output_path, callback=bar, parallelism=parallelism)

@snakemake_compatible(mapping={
    "current_path": "input.current",
    "scenario_path": "params.scenario",
    "parallelism": "threads",
    "output_path": "output[0]",
})
def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an area difference map.")
    parser.add_argument(
        '--current',
        type=Path,
        help='Path of current fractional maps',
        required=True,
        dest='current_path',
    )
    parser.add_argument(
        '--scenario',
        type=Path,
        help='Path of the scenario fractional maps',
        required=True,
        dest='scenario_path',
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Path where final map should be stored',
        required=True,
        dest='output_path',
    )
    parser.add_argument(
        '-j',
        type=int,
        help='Number of concurrent threads to use for calculation.',
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

    make_diff_map(
        args.current_path,
        args.scenario_path,
        args.output_path,
        args.parallelism,
        args.show_progress,
    )

if __name__ == "__main__":
    main()
