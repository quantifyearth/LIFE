import argparse
import math
import os
import sys
from enum import Enum

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

def calc_persistence_value(current_AOH: float, historic_AOH: float, exponent_func) -> float:
    sp_P = exponent_func(current_AOH / historic_AOH)
    sp_P_fix = np.where(sp_P > 1, 1, sp_P)
    return sp_P_fix

def process_delta_p(current: RasterLayer, scenario: RasterLayer, current_AOH: float, historic_AOH: float, exponent_func_raster) -> RasterLayer:
    # In theory we could recalc current_AOH, but given we already have it don't duplicate work
    # New section added in: Calculating for rasters rather than csv's
    const_layer = ConstantLayer(current_AOH) # MAKE A LAYER WITH THE SAME PROPERTIES AS CURRENT AOH RASTER BUT FILLED WITH THE CURRENT AOH
    calc_1 = (const_layer - current) + scenario # FIRST CALCULATION : NEW AOH
    new_AOH = RasterLayer.empty_raster_layer_like(current)
    calc_1.save(new_AOH)

    calc_2 = (new_AOH / historic_AOH).numpy_apply(exponent_func_raster)
    calc_2 = calc_2.numpy_apply(lambda chunk: np.where(chunk > 1, 1, chunk))
    new_p = RasterLayer.empty_raster_layer_like(new_AOH)
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
                print(f"Failed to open scenario layer {os.path.join(scenario_aohs_path, filename)}")
                sys.exit()
            try:
                historic_AOH = RasterLayer.layer_from_file(os.path.join(historic_aohs_path, filename)).sum()
            except FileNotFoundError as fnf:
                print(f"Failed to open historic layer {os.path.join(historic_aohs_path, filename)}")
                sys.exit()

            if historic_AOH == 0.0:
                print(f"Historic AoH for {taxid} is zero, aborting")
                sys.exit()

            print(f"current: {current.sum()}\nscenario: {scenario.sum()}\nhistoric: {historic_AOH.sum()}")

            layers = [current, scenario]
            union = RasterLayer.find_union(layers)
            for layer in layers:
                try:
                    layer.set_window_for_union(union)
                except ValueError:
                    pass

            current_AOH = current.sum()

            new_p_layer = process_delta_p(current, scenario, current_AOH, historic_AOH, z_exponent_func_raster)
            print(new_p_layer.sum())

            old_persistence = calc_persistence_value(current_AOH, historic_AOH, z_exponent_func_float)
            print(old_persistence)
            calc = new_p_layer - ConstantLayer(old_persistence)
            delta_p = RasterLayer.empty_raster_layer_like(new_p_layer, filename=os.path.join(output_folder, filename))
            calc.save(delta_p)

        case Season.NONBREEDING:
            nonbreeding_filename = f"{taxid}_{Season.NONBREEDING.name}.tif"
            breeding_filename = f"{taxid}_{Season.BREEDING.name}.tif"

            try:
                historic_AOH_breeding = RasterLayer.layer_from_file(os.path.join(historic_aohs_path, breeding_filename)).sum()
                if historic_AOH_breeding == 0.0:
                    print(f"Historic AoH breeding for {taxid} is zero, aborting")
                    sys.exit()
            except FileNotFoundError:
                print(f"Historic AoH for breeding {taxid} not found, aborting")
                sys.exit()
            try:
                historic_AOH_non_breeding = RasterLayer.layer_from_file(os.path.join(historic_aohs_path, nonbreeding_filename)).sum()
                if historic_AOH_non_breeding == 0.0:
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
            except FileNotFoundError as fnf:
                print(f"Failed to open current breeding {os.path.join(current_aohs_path, breeding_filename)}")
                sys.exit()
            try:
                current_non_breeding = open_layer_as_float64(os.path.join(current_aohs_path, nonbreeding_filename))
            except FileNotFoundError as fnf:
                print(f"Failed to open current non breeding {os.path.join(current_aohs_path, nonbreeding_filename)}")
                sys.exit()
            try:
                scenario_breeding = open_layer_as_float64(breeding_scenario_path)
            except FileNotFoundError as fnf:
                print(f"Failed to open scenario breeding {breeding_scenario_path}")
                sys.exit()
            try:
                scenario_non_breeding = open_layer_as_float64(non_breeding_scenario_path)
            except FileNotFoundError as fnf:
                print(f"Failed to open sceario non breeding{fnf.filename}")
                sys.exit()

            layers = [current_breeding, current_non_breeding, scenario_breeding, scenario_non_breeding]
            union = RasterLayer.find_union(layers)
            for layer in layers:
                try:
                    layer.set_window_for_union(union)
                except ValueError:
                    pass

            current_AOH_breeding = current_breeding.sum()
            persistence_breeding = calc_persistence_value(current_AOH_breeding, historic_AOH_breeding, z_exponent_func_float)

            current_AOH_non_breeding = current_non_breeding.sum()
            persistence_non_breeding = calc_persistence_value(current_AOH_non_breeding, historic_AOH_non_breeding, z_exponent_func_float)

            old_persistence = (persistence_breeding ** 0.5) * (persistence_non_breeding ** 0.5)

            new_p_breeding = process_delta_p(current_breeding, scenario_breeding, current_AOH_breeding, historic_AOH_breeding, z_exponent_func_raster)
            new_p_non_breeding = process_delta_p(current_non_breeding, scenario_non_breeding, current_AOH_non_breeding, historic_AOH_non_breeding, z_exponent_func_raster)

            new_p_layer = (new_p_breeding ** 0.5) * (new_p_non_breeding ** 0.5)

            delta_p_layer = new_p_layer - ConstantLayer(old_persistence)

            output = RasterLayer.empty_raster_layer_like(new_p_breeding, filename=os.path.join(output_folder, nonbreeding_filename))
            delta_p_layer.save(output)

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
