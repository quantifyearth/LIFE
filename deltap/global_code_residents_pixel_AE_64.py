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
import sys
import pandas as pd
import numpy as np
import argparse
import os
import warnings

from osgeo import gdal
from yirgacheffe.layers import RasterLayer, ConstantLayer


quiet = True
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
   
try:
    current32 = RasterLayer.layer_from_file(args['current_path'])
    current = RasterLayer.empty_raster_layer_like(current32, datatype=gdal.GDT_Float64)
    current32.save(current) # think of save as 'save into' - this saves current32 into current

    current_AOH = current.sum() 
except FileNotFoundError:
    if quiet:
        quit()
    else:
        quit("Current AOH file {args['current_path']} not found, aborting.")
try:
    scenario32 = RasterLayer.layer_from_file(args['scenario_path'])
    scenario = RasterLayer.empty_raster_layer_like(scenario32, datatype=gdal.GDT_Float64)
    scenario32.save(scenario)

except FileNotFoundError:
    if quiet:
        quit()
    else:
        quit("Scenario AOH file {args['scenario_path']} not found, aborting.")
if args["hist_table"] != None:
    """ This only works with /maps/results/global_analysis/processed/PNV_AOH.csv"""
    hdf = pd.read_csv(args["hist_table"])
    taxid = os.path.split(args["current_path"])[-1].split("-")[-1].split(".")[0]
    seas = os.path.split(args["current_path"])[-1].split("-")[0].split(".")[-1].lower().strip(" ")
    historic_AOH = hdf[(hdf.id_no == int(taxid))&(hdf.season == " " + seas)].AOH.values[0]
else:
    try:
        historic32 = RasterLayer.layer_from_file(args['historic_path'])
        historic = RasterLayer.empty_raster_layer_like(historic32, datatype=gdal.GDT_Float64)
        historic32.save(historic)

        historic_AOH = historic.sum()
    except FileNotFoundError:
        if quiet:
            quit()
        else:
            quit("Warning: missing csv for historic aoh - skipping species. This is probably due to artificial hab-preference.")


def global_p_calc(current_AOH,historic_AOH, exponent):
    sp_P = (current_AOH / historic_AOH)**exponent
    sp_P_fix = np.where(sp_P > 1, 1, sp_P)
    return sp_P_fix 

persistence = global_p_calc(current_AOH,historic_AOH,0.25)
print(persistence)


# New section added in: Calculating for rasters rather than csv's
const_layer = ConstantLayer(current_AOH) # MAKE A LAYER WITH THE SAME PROPERTIES AS CURRENT AOH RASTER BUT FILLED WITH THE CURRENT AOH 
calc_1 = (const_layer - current) + scenario # FIRST CALCULATION : NEW AOH 
new_AOH = RasterLayer.empty_raster_layer_like(current)
calc_1.save(new_AOH)


calc_2 = ((new_AOH / historic_AOH) ** 0.25) #SECOND CALCULATION: NEW PERSISTENCE
calc_2 = calc_2.numpy_apply(lambda chunk: np.where(chunk > 1, 1, chunk))
new_p = RasterLayer.empty_raster_layer_like(new_AOH)
calc_2.save(new_p)
# calc_2.sum()
# calc_2.save(new_p, and_sum=True)

calc_3 = calc_2 - ConstantLayer(persistence) # CALCULATION 3 : DELTA P
delta_p = RasterLayer.empty_raster_layer_like(new_p, filename=args['output_path'])
calc_3.save(delta_p)
