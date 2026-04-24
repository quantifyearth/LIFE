import argparse
import json
import math
import os
import sys
from pathlib import Path

from snakemake_argparse_bridge import snakemake_compatible # type: ignore

os.environ['YIRGACHEFFE_BACKEND'] = 'NUMPY'
import yirgacheffe as yg # pylint: disable=C0413

# This isn't a hard requirement, but in practice most experiments use 0.25, and the original
# paper used the other three values for comparison. Other values are valid, but to save wasted
# times with typos, we do restrict this to the subset used for the paper.
FLOAT_EXPONENTS = {0.1, 0.25, 0.5, 1.0}

GOMPERTZ_A = 2.5
GOMPERTZ_B = -14.5
GOMPERTZ_ALPHA = 1

def open_layer(filename: Path) -> tuple[yg.YirgacheffeLayer,float]:
    """We use this helper function for two reasons:
    1. The delta-p values are quite small, and so we want to ensure things are in float64.
    2. We almost always need the total area, but rather than calculate it we can get that
       from the JSON file that sits besides the TIFF.
    """
    # The "nan" is an artefact of bouncing the data via pandas
    if filename.name == "nan":
        return yg.constant(0.0), 0.0

    layer = yg.read_raster(filename)

    json_filename = filename.parent / f"{filename.stem}.json"
    with open(json_filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    total_aoh = data["aoh_total"]

    return layer, total_aoh

def calc_persistence_value(
    current_aoh: float,
    historic_aoh: float,
    exponent: str | float
) -> float:
    scaled_aoh = current_aoh / historic_aoh
    if isinstance(exponent, float):
        sp_p = scaled_aoh ** exponent
    else:
        assert exponent == "gompetz"
        sp_p = math.exp(-math.exp(GOMPERTZ_A + (GOMPERTZ_B * (scaled_aoh ** GOMPERTZ_ALPHA))))
    return 1.0 if sp_p > 1.0 else sp_p

def process_delta_p(
    current: yg.YirgacheffeLayer,
    scenario: yg.YirgacheffeLayer | float,
    current_aoh: float,
    historic_aoh: float,
    exponent: str | float
) -> yg.YirgacheffeLayer:

    new_aoh = (current_aoh - current) + scenario

    scaled_aoh = new_aoh / historic_aoh
    if isinstance(exponent, float):
        calc_2 = scaled_aoh ** exponent
    else:
        assert exponent == "gompetz"
        calc_2 = yg.exp(-yg.exp(GOMPERTZ_A + (GOMPERTZ_B * (scaled_aoh ** GOMPERTZ_ALPHA))))
    new_p = yg.where(calc_2 > 1, 1, calc_2)

    return new_p

def global_code_residents_pixel_ae(
    taxid: str,
    season: str,
    current_aohs_path: Path,
    scenario_aohs_path: Path,
    historic_aohs_path: Path,
    exponent: str | float,
    output_path: Path,
) -> None:
    os.makedirs(output_path.parent, exist_ok=True)

    # snakemake demands we write a file to show we've done something, even if there
    # is no tiff generated
    sentinel_path = output_path.parent / f".{taxid}_{season}.done"

    match season:
        case "RESIDENT":
            filename = f"aoh_{taxid}_{season}.tif"
            try:
                current, current_aoh = open_layer(current_aohs_path / filename)
            except FileNotFoundError:
                print(f"Failed to open current layer {current_aohs_path / filename}", file=sys.stderr)
                sentinel_path.touch()
                return

            try:
                scenario: yg.YirgacheffeLayer | float
                scenario, _ = open_layer(scenario_aohs_path / filename)
            except FileNotFoundError:
                # If there is a current but now scenario file it's because the species went extinct under the scenario
                scenario = 0.0

            try:
                _, historic_aoh = open_layer(historic_aohs_path / filename)
            except FileNotFoundError:
                print(f"Failed to open historic layer {historic_aohs_path / filename}", file=sys.stderr)
                sentinel_path.touch()
                return

            if historic_aoh == 0.0:
                print(f"Historic AoH for {taxid} is zero, skipping", file=sys.stderr)
                sentinel_path.touch()
                return

            old_persistence = calc_persistence_value(current_aoh, historic_aoh, exponent)


            # In general Yirgacheffe can infer the behaviour needed for area intersections based on
            # operator, but in this instance we want to force the caclulation to take place for the
            # union of the areas involved.
            layers = [x for x in [current, scenario] if isinstance(x, yg.YirgacheffeLayer)]
            union = yg.layers.RasterLayer.find_union(layers)
            for layer in layers:
                layer.set_window_for_union(union)

            new_p_layer = process_delta_p(current, scenario, current_aoh, historic_aoh, exponent)

            delta_p = new_p_layer - old_persistence

            try:
                delta_p.to_geotiff(output_path)
            except ValueError:
                print(f"Failed to align layers for {taxid}_{season}", file=sys.stderr)
                sentinel_path.touch()
                return

            sentinel_path.touch()

        case "NONBREEDING":
            nonbreeding_filename = f"aoh_{taxid}_NONBREEDING.tif"
            breeding_filename = f"aoh_{taxid}_BREEDING.tif"

            try:
                _, historic_aoh_breeding = open_layer(historic_aohs_path / breeding_filename)
                if historic_aoh_breeding == 0.0:
                    print(f"Historic AoH breeding for {taxid} is zero, skipping", file=sys.stderr)
                    sentinel_path.touch()
                    return
            except FileNotFoundError:
                print(f"Historic AoH for breeding {taxid} not found, skipping", file=sys.stderr)
                sentinel_path.touch()
                return
            try:
                _, historic_aoh_non_breeding = open_layer(historic_aohs_path / nonbreeding_filename)
                if historic_aoh_non_breeding == 0.0:
                    print(f"Historic AoH for non breeding {taxid} is zero, skipping", file=sys.stderr)
                    sentinel_path.touch()
                    return
            except FileNotFoundError:
                print(f"Historic AoH for non breeding {taxid} not found, skipping", file=sys.stderr)
                sentinel_path.touch()
                return

            if scenario_aohs_path.name != "nan":
                non_breeding_scenario_path = scenario_aohs_path / nonbreeding_filename
                breeding_scenario_path = scenario_aohs_path / breeding_filename
            else:
                non_breeding_scenario_path = Path("nan") # nan path is the sentinel from csv inputs
                breeding_scenario_path = Path("nan")

            try:
                current_breeding, current_aoh_breeding = open_layer(current_aohs_path / breeding_filename)
            except FileNotFoundError:
                print(f"Failed to open current breeding {current_aohs_path / breeding_filename}", file=sys.stderr)
                sentinel_path.touch()
                return
            try:
                current_non_breeding, current_aoh_non_breeding = open_layer(current_aohs_path / nonbreeding_filename)
            except FileNotFoundError:
                print(f"Failed to open current non breeding {current_aohs_path / nonbreeding_filename}",
                    file=sys.stderr)
                sentinel_path.touch()
                return
            try:
                scenario_breeding: yg.YirgacheffeLayer | float
                scenario_breeding, _ = open_layer(breeding_scenario_path)
            except FileNotFoundError:
                # If there is a current but now scenario file it's because the species went extinct under the scenario
                scenario_breeding = 0.0
            try:
                scenario_non_breeding: yg.YirgacheffeLayer | float
                scenario_non_breeding, _ = open_layer(non_breeding_scenario_path)
            except FileNotFoundError:
                # If there is a current but now scenario file it's because the species went extinct under the scenario
                scenario_non_breeding = 0.0

            persistence_breeding = calc_persistence_value(
                current_aoh_breeding,
                historic_aoh_breeding,
                exponent,
            )

            persistence_non_breeding = calc_persistence_value(
                current_aoh_non_breeding,
                historic_aoh_non_breeding,
                exponent,
            )

            old_persistence = (persistence_breeding ** 0.5) * (persistence_non_breeding ** 0.5)

            # In general Yirgacheffe can infer the behaviour needed for area intersections based on
            # operator, but in this instance we want to force the calculation to take place for the
            # union of the areas involved.
            src_layers = [current_breeding, scenario_breeding, current_non_breeding, scenario_non_breeding]
            layers = [x for x in src_layers if isinstance(x, yg.YirgacheffeLayer)]
            union = yg.layers.RasterLayer.find_union(layers)
            for layer in layers:
                layer.set_window_for_union(union)

            new_p_breeding = process_delta_p(
                current_breeding,
                scenario_breeding,
                current_aoh_breeding,
                historic_aoh_breeding,
                exponent,
            )
            new_p_non_breeding = process_delta_p(
                current_non_breeding,
                scenario_non_breeding,
                current_aoh_non_breeding,
                historic_aoh_non_breeding,
                exponent,
            )
            new_p_layer = (new_p_breeding ** 0.5) * (new_p_non_breeding ** 0.5)

            delta_p_layer = new_p_layer - old_persistence

            try:
                delta_p_layer.to_geotiff(output_path)
            except ValueError:
                print(f"Failed to align layers for {taxid}_{season}", file=sys.stderr)
                sentinel_path.touch()
                return

            sentinel_path.touch()
        case "BREEDING":
            # covered by the nonbreeding case
            sentinel_path.touch()
        case _:
            sentinel_path.touch()
            sys.exit(f"Unexpected season for species {taxid}: {season}")

def exponent_type(value: str):
    if value == "gompertz":
        return value
    try:
        f = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid exponent: {value!r}") from exc
    if f not in FLOAT_EXPONENTS:
        raise argparse.ArgumentTypeError(f"numeric exponent must be one of {sorted(FLOAT_EXPONENTS)}, got {f}")
    return f

@snakemake_compatible(mapping={
    "taxid": "params.taxon_id",
    "season": "params.season",
    "current_path": "params.current_path",
    "historic_path": "params.pnv_path",
    "scenario_path": "params.scenario_path",
    "output_path": "params.output_tif",
    "exponent": "params.curve",
})
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--taxid',
        type=str,
        required=True,
        dest='taxid',
        help="Species taxon id",
    )
    parser.add_argument(
        '--season',
        type=str,
        required=True,
        dest='season',
        help="Seasonality",
    )
    parser.add_argument(
        '--current_path',
        type=Path,
        required=True,
        dest="current_path",
        help="path to species current AOH hex"
    )
    parser.add_argument(
        '--scenario_path',
        type=Path,
        required=True,
        dest="scenario_path",
        help="path to species scenario AOH hex"
    )
    parser.add_argument(
        '--historic_path',
        type=Path,
        required=True,
        dest="historic_path",
        help="path to species historic AOH hex"
    )
    parser.add_argument('--output_path',
        type=Path,
        required=True,
        dest="output_path",
        help="path to save output tif"
    )
    parser.add_argument(
        '--z',
        dest='exponent',
        type=exponent_type,
        default=0.25
    )
    args = parser.parse_args()

    global_code_residents_pixel_ae(
        args.taxid,
        args.season,
        args.current_path,
        args.scenario_path,
        args.historic_path,
        args.exponent,
        args.output_path,
    )

if __name__ == "__main__":
    main()
