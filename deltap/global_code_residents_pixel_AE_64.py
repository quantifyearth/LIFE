# -*- coding: utf-8 -*-
"""
Created on Wed May 17 15:51:13 2023

@authors: Thomas Ball, Ali Eyres

This is a modified version of global_code_residents.py that calculates delta p for a set of
Rasters. Any resolution should work since it just uses x/y as identifiers.

# AE modified from TB's Code for CSVs
# Not sure if it works...

This isn't tidied or commented properly
"""
import argparse
import os
import re
import sys
import warnings

# warnings.simplefilter("error")

import pandas as pd
import numpy as np
from osgeo import gdal
from yirgacheffe.layers import RasterLayer, ConstantLayer


quiet = False
overwrite = True

parser = argparse.ArgumentParser()
parser.add_argument('--current_path',
    type=str,required=True,dest="current_path",
    help="path to species current AOH hex")
parser.add_argument('--historic_path',
    type=str,required=True,dest="historic_path",
    help="path to species historic AOH hex")
parser.add_argument('--scenario_path',
    type=str,required=True,dest="scenario_path",
    help="path to species scenario AOH hex")
parser.add_argument('--output_path',
    type=str,required=True,dest="output_path",
    help="path to save output csv")
parser.add_argument('-ht', '--hist_table',
                    dest = "hist_table",
                    type = str)
args = vars(parser.parse_args())

if not overwrite and os.path.isfile(args['output_path']):
    quit(f"{args['output_path']} exists, set overwrite to False to ignore this.")

FILERE = re.compile(r'.*Seasonality.(\w+)-(\d+).tif$')
season, taxid = FILERE.match(args['current_path']).groups()
season = season.lower()


def open_layer_as_float64(filename: str) -> RasterLayer:
    layer = RasterLayer.layer_from_file(filename)
    if layer.datatype == gdal.GDT_Float64:
        return layer
    layer64 = RasterLayer.empty_raster_layer_like(layer, datatype=gdal.GDT_Float64)
    layer.save(layer64)
    return layer64


def calc_persistence_value(current_AOH: float, historic_AOH: float, exponent: float) -> float:
    sp_P = (current_AOH / historic_AOH) ** exponent
    sp_P_fix = np.where(sp_P > 1, 1, sp_P)
    return sp_P_fix


def process_delta_p(current: RasterLayer, scenario: RasterLayer, current_AOH: float, historic_AOH: float) -> RasterLayer:
    # In theory we could recalc current_AOH, but given we already have it don't duplicate work

    # New section added in: Calculating for rasters rather than csv's
    const_layer = ConstantLayer(current_AOH) # MAKE A LAYER WITH THE SAME PROPERTIES AS CURRENT AOH RASTER BUT FILLED WITH THE CURRENT AOH
    calc_1 = (const_layer - current) + scenario # FIRST CALCULATION : NEW AOH
    new_AOH = RasterLayer.empty_raster_layer_like(current)
    calc_1.save(new_AOH)

    calc_2 = ((new_AOH / historic_AOH) ** 0.25) #SECOND CALCULATION: NEW PERSISTENCE
    calc_2 = calc_2.numpy_apply(lambda chunk: np.where(chunk > 1, 1, chunk))
    new_p = RasterLayer.empty_raster_layer_like(new_AOH)
    calc_2.save(new_p)

    return new_p


hdf = pd.read_csv(args['hist_table'])

if season == 'resident':
    try:
        current = open_layer_as_float64(args['current_path'])
        scenario = open_layer_as_float64(args['scenario_path'])
    except FileNotFoundError as fnf:
        quit(f"Failed to open {fnf.filename}")

    layers = [current, scenario]
    union = RasterLayer.find_union(layers)
    for layer in layers:
        layer.set_window_for_union(union)

    current_AOH = current.sum()
    historic_AOH = hdf[(hdf.id_no == int(taxid))&(hdf.season == " " + season)].AOH.values[0]
    if historic_AOH == 0.0:
        quit(f"Historic AoH for {taxid} is zero, aborting")

    new_p_layer = process_delta_p(current, scenario, current_AOH, historic_AOH)

    old_persistence = calc_persistence_value(current_AOH, historic_AOH, 0.25)
    calc = new_p_layer - ConstantLayer(old_persistence)
    delta_p = RasterLayer.empty_raster_layer_like(new_p_layer, filename=args['output_path'])
    calc.save(delta_p)

elif season == 'nonbreeding':
    # We have the nonbreeding path, work out the breeding path, check that works, and then do the work.
    non_breeding_current_path = args['current_path']
    directory, _ = os.path.split(non_breeding_current_path)
    breeding_current_path = os.path.join(directory, f'Seasonality.BREEDING-{taxid}.tif')

    non_breeding_scenario_path = args['scenario_path']
    assert 'NONBREEDING' in non_breeding_scenario_path
    directory, _ = os.path.split(non_breeding_scenario_path)
    breeding_scenario_path = os.path.join(directory, f'Seasonality.BREEDING-{taxid}.tif')

    try:
        current_breeding = open_layer_as_float64(breeding_current_path)
        current_non_breeding = open_layer_as_float64(non_breeding_current_path)
        scenario_breeding = open_layer_as_float64(breeding_scenario_path)
        scenario_non_breeding = open_layer_as_float64(non_breeding_scenario_path)
    except FileNotFoundError as fnf:
        quit(f"Failed to open {fnf.filename}")

    layers = [current_breeding, current_non_breeding, scenario_breeding, scenario_non_breeding]
    union = RasterLayer.find_union(layers)
    for layer in layers:
        layer.set_window_for_union(union)


    current_AOH_breeding = current_breeding.sum()
    historic_AOH_breeding = hdf[(hdf.id_no == int(taxid))&(hdf.season == " " + 'breeding')].AOH.values[0]
    if historic_AOH_breeding == 0.0:
        quit(f"Historic AoH breeding for {taxid} is zero, aborting")
    persistence_breeding = calc_persistence_value(current_AOH_breeding, historic_AOH_breeding, 0.25)

    current_AOH_non_breeding = current_non_breeding.sum()
    historic_AOH_non_breeding = hdf[(hdf.id_no == int(taxid))&(hdf.season == " " + 'nonbreeding')].AOH.values[0]
    if historic_AOH_non_breeding == 0.0:
        quit(f"Historic AoH for non breeding {taxid} is zero, aborting")
    persistence_non_breeding = calc_persistence_value(current_AOH_non_breeding, historic_AOH_non_breeding, 0.25)

    old_persistence = (persistence_breeding ** 0.5) * (persistence_non_breeding ** 0.5)

    new_p_breeding = process_delta_p(current_breeding, scenario_breeding, current_AOH_breeding, historic_AOH_breeding)
    new_p_non_breeding = process_delta_p(current_non_breeding, scenario_non_breeding, current_AOH_non_breeding, historic_AOH_non_breeding)

    new_p_layer = (new_p_breeding ** 0.5) * (new_p_non_breeding ** 0.5)

    delta_p_layer = new_p_layer - ConstantLayer(old_persistence)

    output = RasterLayer.empty_raster_layer_like(new_p_breeding, filename=args['output_path'])
    delta_p_layer.save(output)

