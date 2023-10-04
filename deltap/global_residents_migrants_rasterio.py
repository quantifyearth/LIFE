# -*- coding: utf-8 -*-
"""
Created on Wed May 17 15:51:13 2023

@authors: Thomas Ball, Ali Eyres

This is a modified version of global_code_residents.py that calculates delta p for a set of 
Rasters. Any resolution and set of shapes should work, the output raster's extent should
cover the extents of all inputs.

# AE modified from TB's Code for CSVs
# Not sure if it works... 

This isn't tidied or commented properly. Also doesn't work for historic rasters, you need to
use the .csv of historic values (probably "/maps/results/global_analysis/processed/PNV_AOH.csv")
"""
import sys
import pandas as pd
import numpy as np
import argparse
import os
import warnings
import rasterio

quiet = True
overwrite = True
exponent = 0.25 

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

def harmonise(rasters):
    data_list = []
    extents = []
    for raster in rasters:
        data = raster.read(1)
        data_list.append(data)
        extents.append(raster.bounds)
        crs = raster.crs
    max_extent = (
        min(extent.left for extent in extents),
        min(extent.bottom for extent in extents),
        max(extent.right for extent in extents),
        max(extent.top for extent in extents)
    )
    width = round((max_extent[2] - max_extent[0]) / raster.res[0])
    height = round((max_extent[3] - max_extent[1]) / raster.res[1])
    shape = (height, width)
    arrays = [np.zeros((height, width), dtype=np.float64) for _ in range(len(rasters))]
    offsets_x = []
    offsets_y = []
    bounds = rasterio.coords.BoundingBox(left=max_extent[0],bottom=max_extent[1],
                                         right=max_extent[2],top=max_extent[3])
    transform = rasterio.transform.from_bounds(*bounds, width, height)
    window = rasterio.windows.from_bounds(*bounds, transform=transform)
    for extent in extents:
        offset_x = abs(round((extent.left - max_extent[0]) / raster.res[0]))
        offset_y = abs(round((extent.bottom - max_extent[1]) / raster.res[1]))
        offsets_x.append(offset_x)
        offsets_y.append(offset_y)
    for i, data in enumerate(data_list):
        arrays[i][offsets_y[i]:offsets_y[i]+data.shape[0], offsets_x[i]:offsets_x[i]+data.shape[1]] = data
    return arrays, transform, shape, crs, window

def global_p_calc(current_AOH,historic_AOH, exponent):
    sp_P = (current_AOH / historic_AOH)**exponent
    sp_P_fix = np.where(sp_P > 1, 1, sp_P)
    return sp_P_fix 

def sillygoofyrenamingfunction(fname):
        return fname.split(".")[0] + ".BREEDING-" + fname.split("-")[-1]

if not overwrite and os.path.isfile(args['output_path']):
    quit(f"{args['output_path']} exists, set overwrite to False to ignore this.")
   
if os.path.isfile(args["current_path"]) == False:
    if quiet:
        quit()
    else:
        quit("Current AOH file {args['current_path']} not found, aborting.")

if os.path.isfile(args["scenario_path"]) == False:
    if quiet:
        quit()
    else:
        quit("Scenario AOH file {args['scenario_path']} not found, aborting.")

current_ds = rasterio.open(args["current_path"])
scenario_ds = rasterio.open(args["scenario_path"])

if args["hist_table"] != None:
    """ This only works with /maps/results/global_analysis/processed/PNV_AOH.csv"""
    hdf = pd.read_csv(args["hist_table"])
    taxid = os.path.split(args["current_path"])[-1].split("-")[-1].split(".")[0]
    seas = os.path.split(args["current_path"])[-1].split("-")[0].split(".")[-1].lower().strip(" ")
    try:
        historic_AOH = hdf[(hdf.id_no == int(taxid))&(hdf.season == " " + seas)].AOH.values[0]
    except IndexError:
        if quiet:
            quit()
        else:
            quit("Warning: missing historic aoh in provided .csv - skipping species. This is probably due to artificial hab-preference.")
else: 
    quit("THIS IS CURRENTLY REDUNDANT - USE THE PNV CSV FOR HISTORIC AOHs!!!!")
    try: 
        historic_ds = rasterio.open(args["historic_path"])
    except FileNotFoundError:
        if quiet:
            quit()
        else:
            quit("Warning: missing csv for historic aoh - skipping species. This is probably due to artificial hab-preference.")

seas = os.path.split(args["current_path"])[-1].split("-")[0].split(".")[-1].lower().strip(" ")
if "resident" in seas:
    (current_arr, scenario_arr), transform, shape, crs, window = harmonise(
        [current_ds, scenario_ds])
    current_AOH = current_arr.sum()
    if historic_AOH == 0:
        if quiet:
            quit()
        else:
            quit("Warning: missing historic aoh in csv - skipping species. This is probably due to artificial hab-preference.")
    persistence = global_p_calc(current_AOH,historic_AOH,exponent)
    curr_mask = (current_arr != 0).astype(int)
    new_aoh = (curr_mask * current_AOH) - current_arr + scenario_arr
    new_p = (new_aoh / historic_AOH) ** exponent
    new_p[new_p > 1] = 1
    np_mask = (new_p != 0).astype(int)
    deltap = new_p - (persistence * np_mask)
    with rasterio.open(
        args["output_path"], "w", driver="GTiff",
        height=shape[0], width=shape[1], 
        count=1, dtype=np.float64,
        crs=crs, transform=transform,
    ) as dst:
        dst.write(deltap, indexes=1)

if "nonbreeding" in seas:
    current_nb_ds = current_ds
    historic_AOH_nb = historic_AOH
    scenario_nb_ds = scenario_ds
    curr_base,curr_fname = os.path.split(args["current_path"])
    br_curr_path = os.path.join(curr_base, sillygoofyrenamingfunction(curr_fname))
    if args["hist_table"] != None:
        """ This only works with /maps/results/global_analysis/processed/PNV_AOH.csv"""
        hdf = pd.read_csv(args["hist_table"])
        taxid = os.path.split(args["current_path"])[-1].split("-")[-1].split(".")[0]
        seas = "breeding"
        try:
            historic_AOH_br = hdf[(hdf.id_no == int(taxid))&(hdf.season == " " + seas)].AOH.values[0]
        except IndexError:
            if quiet:
                quit()
            else:
                quit("Warning: missing (at least one) historic aoh in PNV csv - skipping species. This is probably due to artificial hab-preference.")
    else:
        quit("THIS IS CURRENTLY REDUNDANT - USE THE PNV CSV FOR HISTORIC AOHs!!!!")
        hist_base,hist_fname = os.path.split(args["historic_path"])
        br_hist_path = os.path.join(hist_base, sillygoofyrenamingfunction(hist_fname))
        historic32_br = RasterLayer.layer_from_file(br_hist_path)
        historic_br = RasterLayer.empty_raster_layer_like(historic32_br, datatype=gdal.GDT_Float64)
        historic32_br.save(historic_br)
        historic_AOH_br = historic_br.sum()
    scen_base,scen_fname = os.path.split(args["scenario_path"])
    br_curr_path = os.path.join(curr_base, sillygoofyrenamingfunction(curr_fname))
    br_scen_path = os.path.join(scen_base, sillygoofyrenamingfunction(scen_fname))
    if os.path.isfile(br_scen_path) == False or os.path.isfile(br_curr_path) == False:
        if quiet:
            quit()
        else:
            taxid = os.path.split(args["current_path"])[-1].split("-")[-1].split(".")[0]
            quit("Warning: Can't find the corresponding breeding range raster for {taxid}, skipping...")
    scenario_br_ds = rasterio.open(br_scen_path)
    current_br_ds = rasterio.open(br_curr_path)
    (br_curr, nb_curr, br_scen, nb_scen), transform, shape, crs, window = harmonise(
        [current_br_ds, current_nb_ds, scenario_br_ds, scenario_nb_ds])
    
    new_aoh_br = (br_curr.sum() * (br_curr != 0).astype(int)) - br_curr + br_scen
    new_aoh_nb = (nb_curr.sum() * (nb_curr != 0).astype(int)) - nb_curr + nb_scen

    if historic_AOH_br == 0 or historic_AOH_nb == 0:
        if quiet:
            quit()
        else:
            quit("Warning: missing (at least one) historic aoh in csv - skipping species. This is probably due to artificial hab-preference.")
    new_p_br = (new_aoh_br / historic_AOH_br) ** exponent
    new_p_br[new_p_br > 1] = 1
    new_p_nb = (new_aoh_nb / historic_AOH_nb) ** exponent
    new_p_nb[new_p_nb > 1] = 1 
    new_p = (new_p_br ** 0.5) * (new_p_nb ** 0.5)
    old_p = (((br_curr.sum()/historic_AOH_br)**exponent)**0.5) * (((nb_curr.sum()/historic_AOH_nb)**exponent)**0.5)
    np_mask = (new_p != 0).astype(int)
    deltap = new_p - (old_p * np_mask) 
    with rasterio.open(
        args["output_path"], "w", driver="GTiff",
        height = shape[0], width=shape[1], 
        count=1,  # Number of bands
        dtype=np.float64,
        crs=crs, transform=transform,
    ) as dst:
        dst.write(deltap, window=window, indexes=1)