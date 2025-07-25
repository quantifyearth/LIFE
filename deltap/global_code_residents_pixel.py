import argparse
import math
import os
import shutil
import sys
from enum import Enum
from tempfile import TemporaryDirectory

import geopandas as gpd
import numpy as np
from osgeo import gdal
from yirgacheffe.layers import RasterLayer, ConstantLayer

GOMPERTZ_A = 2.5
GOMPERTZ_B = -14.5
GOMPERTZ_ALPHA = 1

class Season(Enum):
    RESIDENT = 1
    BREEDING = 2
    NONBREEDING = 3

def gen_gompertz(x: float) -> float:
    return math.exp(-math.exp(GOMPERTZ_A + (GOMPERTZ_B * (x ** GOMPERTZ_ALPHA))))

def numpy_gompertz(x: float) -> float:
    return np.exp(-np.exp(GOMPERTZ_A + (GOMPERTZ_B * (x ** GOMPERTZ_ALPHA))))

def open_layer_as_float64(filename: str) -> RasterLayer:
    if filename == "nan":
        return ConstantLayer(0.0)
    layer = RasterLayer.layer_from_file(filename)
    if layer.datatype == gdal.GDT_Float64:
        return layer
    layer64 = RasterLayer.empty_raster_layer_like(layer, datatype=gdal.GDT_Float64)
    layer.save(layer64)
    return layer64

def calc_persistence_value(current_aoh: float, historic_aoh: float, exponent_func) -> float:
    sp_p = exponent_func(current_aoh / historic_aoh)
    sp_p_fix = 1 if sp_p > 1 else sp_p
    return sp_p_fix

def process_delta_p(
    current: RasterLayer,
    scenario: RasterLayer,
    current_aoh: float,
    historic_aoh: float,
    exponent_func_raster
) -> RasterLayer:
    # In theory we could recalc current_aoh, but given we already have it don't duplicate work
    # New section added in: Calculating for rasters rather than csv's


    new_p = ((ConstantLayer(current_aoh) - current) + scenario) / historic_aoh


    const_layer = ConstantLayer(current_aoh)
    calc_1 = (const_layer - current) + scenario
    new_aoh = RasterLayer.empty_raster_layer_like(current)
    calc_1.save(new_aoh)

    calc_2 = (new_aoh / historic_aoh).numpy_apply(exponent_func_raster)
    calc_2 = calc_2.numpy_apply(lambda chunk: np.where(chunk > 1, 1, chunk))
    new_p = RasterLayer.empty_raster_layer_like(new_aoh)
    calc_2.save(new_p)

    return new_p

def global_code_residents_pixel_ae(
    species_data_path: str,
    current_aohs_path: str,
    scenario_aohs_path: str,
    historic_aohs_path: str,
    exponent: str,
    output_folder: str,
) -> None:
    os.makedirs(output_folder, exist_ok=True)

    os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"
    try:
        filtered_species_info = gpd.read_file(species_data_path)
    except: # pylint:disable=W0702
        sys.exit(f"Failed to read {species_data_path}")
    taxid = filtered_species_info.id_no.values[0]
    season = Season[filtered_species_info.season.values[0]]

    try:
        exp_val = float(exponent)
        z_exponent_func_float = lambda x: np.float_power(x, exp_val)
        z_exponent_func_raster = lambda x: np.float_power(x, exp_val)
    except ValueError:
        if exponent == "gompertz":
            z_exponent_func_float = gen_gompertz
            z_exponent_func_raster = numpy_gompertz
        else:
            sys.exit(f"unrecognised exponent {exponent}")

    match season:
        case Season.RESIDENT:
            filename = f"{taxid}_{season.name}.tif"
            try:
                current = open_layer_as_float64(os.path.join(current_aohs_path, filename))
            except FileNotFoundError:
                print(f"Failed to open current layer {os.path.join(current_aohs_path, filename)}")
                sys.exit()

            try:
                scenario = open_layer_as_float64(os.path.join(scenario_aohs_path, filename))
            except FileNotFoundError:
                # If there is a current but now scenario file it's because the species went extinct under the scenario
                scenario = ConstantLayer(0.0)

            try:
                historic_aoh = RasterLayer.layer_from_file(os.path.join(historic_aohs_path, filename)).sum()
            except FileNotFoundError:
                print(f"Failed to open historic layer {os.path.join(historic_aohs_path, filename)}")
                sys.exit()

            if historic_aoh == 0.0:
                print(f"Historic AoH for {taxid} is zero, aborting")
                sys.exit()

            # print(f"current: {current.sum()}\nscenario: {scenario.sum()}\nhistoric: {historic_aoh.sum()}")

            layers = [current, scenario]
            union = RasterLayer.find_union(layers)
            for layer in layers:
                try:
                    layer.set_window_for_union(union)
                except ValueError:
                    pass

            current_aoh = current.sum()

            new_p_layer = process_delta_p(current, scenario, current_aoh, historic_aoh, z_exponent_func_raster)
            print(new_p_layer.sum())

            old_persistence = calc_persistence_value(current_aoh, historic_aoh, z_exponent_func_float)
            print(old_persistence)
            calc = new_p_layer - ConstantLayer(old_persistence)

            with TemporaryDirectory() as tmpdir:
                tmpfile = os.path.join(tmpdir, filename)
                with RasterLayer.empty_raster_layer_like(new_p_layer, filename=tmpfile) as delta_p:
                    calc.save(delta_p)
                shutil.move(tmpfile, os.path.join(output_folder, filename))

        case Season.NONBREEDING:
            nonbreeding_filename = f"{taxid}_{Season.NONBREEDING.name}.tif"
            breeding_filename = f"{taxid}_{Season.BREEDING.name}.tif"

            try:
                with RasterLayer.layer_from_file(os.path.join(historic_aohs_path, breeding_filename)) as aoh:
                    historic_aoh_breeding = aoh.sum()
                if historic_aoh_breeding == 0.0:
                    print(f"Historic AoH breeding for {taxid} is zero, aborting")
                    sys.exit()
            except FileNotFoundError:
                print(f"Historic AoH for breeding {taxid} not found, aborting")
                sys.exit()
            try:
                with RasterLayer.layer_from_file(os.path.join(historic_aohs_path, nonbreeding_filename)) as aoh:
                    historic_aoh_non_breeding = aoh.sum()
                if historic_aoh_non_breeding == 0.0:
                    print(f"Historic AoH for non breeding {taxid} is zero, aborting")
                    sys.exit()
            except FileNotFoundError:
                print(f"Historic AoH for non breeding {taxid} not found, aborting")
                sys.exit()


            if scenario_aohs_path != "nan":
                non_breeding_scenario_path = os.path.join(scenario_aohs_path, nonbreeding_filename)
                breeding_scenario_path = os.path.join(scenario_aohs_path, breeding_filename)
            else:
                non_breeding_scenario_path = "nan"
                breeding_scenario_path = "nan"

            try:
                current_breeding = open_layer_as_float64(os.path.join(current_aohs_path, breeding_filename))
            except FileNotFoundError:
                print(f"Failed to open current breeding {os.path.join(current_aohs_path, breeding_filename)}")
                sys.exit()
            try:
                current_non_breeding = open_layer_as_float64(os.path.join(current_aohs_path, nonbreeding_filename))
            except FileNotFoundError:
                print(f"Failed to open current non breeding {os.path.join(current_aohs_path, nonbreeding_filename)}")
                sys.exit()
            try:
                scenario_breeding = open_layer_as_float64(breeding_scenario_path)
            except FileNotFoundError:
                # If there is a current but now scenario file it's because the species went extinct under the scenario
                scenario_breeding = ConstantLayer(0.0)
            try:
                scenario_non_breeding = open_layer_as_float64(non_breeding_scenario_path)
            except FileNotFoundError:
                # If there is a current but now scenario file it's because the species went extinct under the scenario
                scenario_non_breeding = ConstantLayer(0.0)

            layers = [current_breeding, current_non_breeding, scenario_breeding, scenario_non_breeding]
            union = RasterLayer.find_union(layers)
            for layer in layers:
                try:
                    layer.set_window_for_union(union)
                except ValueError:
                    pass

            current_aoh_breeding = current_breeding.sum()
            persistence_breeding = calc_persistence_value(
                current_aoh_breeding,
                historic_aoh_breeding,
                z_exponent_func_float
            )

            current_aoh_non_breeding = current_non_breeding.sum()
            persistence_non_breeding = calc_persistence_value(
                current_aoh_non_breeding,
                historic_aoh_non_breeding,
                z_exponent_func_float
            )

            old_persistence = (persistence_breeding ** 0.5) * (persistence_non_breeding ** 0.5)

            new_p_breeding = process_delta_p(
                current_breeding,
                scenario_breeding,
                current_aoh_breeding,
                historic_aoh_breeding,
                z_exponent_func_raster
            )
            new_p_non_breeding = process_delta_p(
                current_non_breeding,
                scenario_non_breeding,
                current_aoh_non_breeding,
                historic_aoh_non_breeding,
                z_exponent_func_raster
            )
            new_p_layer = (new_p_breeding ** 0.5) * (new_p_non_breeding ** 0.5)

            delta_p_layer = new_p_layer - ConstantLayer(old_persistence)

            with TemporaryDirectory() as tmpdir:
                tmpfile = os.path.join(tmpdir, nonbreeding_filename)
                with RasterLayer.empty_raster_layer_like(new_p_breeding, filename=tmpfile) as output:
                    delta_p_layer.save(output)
                shutil.move(tmpfile, os.path.join(output_folder, nonbreeding_filename))

        case Season.BREEDING:
            pass # covered by the nonbreeding case
        case _:
            sys.exit(f"Unexpected season for species {taxid}: {season}")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--speciesdata',
        type=str,
        help="Single species/seasonality geojson",
        required=True,
        dest="species_data_path"
    )
    parser.add_argument(
        '--current_path',
        type=str,
        required=True,
        dest="current_path",
        help="path to species current AOH hex"
    )
    parser.add_argument(
        '--scenario_path',
        type=str,
        required=True,
        dest="scenario_path",
        help="path to species scenario AOH hex"
    )
    parser.add_argument(
        '--historic_path',
        type=str,
        required=False,
        dest="historic_path",
        help="path to species historic AOH hex"
    )
    parser.add_argument('--output_path',
        type=str,
        required=True,
        dest="output_path",
        help="path to save output csv"
    )
    parser.add_argument('--z', dest='exponent', type=str, default='0.25')
    args = parser.parse_args()

    global_code_residents_pixel_ae(
        args.species_data_path,
        args.current_path,
        args.scenario_path,
        args.historic_path,
        args.exponent,
        args.output_path,
    )

if __name__ == "__main__":
    main()
