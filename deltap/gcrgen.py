"""
Generate list of args for use with littlejohn and global_code_residents_pixel.py

Note this works for my file structure specifically to avoid a crazy amount of 
messy kwargs, but can be adapted fairly easily.

"""

import argparse
import os

import pandas as pd

parser = argparse.ArgumentParser(description="")
parser.add_argument('--target_dir',
        type=str,help="Look for folders called 'search' in this directory",
        required=True,dest="target_dir")
parser.add_argument('--scenario', required=True, dest="scenario")
parser.add_argument('--output_dir',
                    type = str, help = "where to save the csv",
                    required = True, dest = "output_dir")
args = vars(parser.parse_args())


classes = ["birds", "mammals", "amphibians", "reptiles"]
z_values = ["gompertz"]
season = "RESIDENT"
habmaps = {"historic"   : "pnv",
           "scenario"   : args["scenario"],
           "current"    : "current_L1L2"
           }
habmaps_r = {v: k for k, v in habmaps.items()}


target_dir = args["target_dir"]

# for z in z_values:
#         for taxa in classes:
#             os.makedirs(os.path.join(args["output_dir"], args["scenario"], str(z), taxa), exist_ok=True)
os.makedirs(args["output_dir"], exist_ok=True)

tif_file_paths = []
for path, subdirs, files in os.walk(args["target_dir"]):
    for name in files:
        _, ext = os.path.splitext(name)
        if ext == '.tif':
            tif_file_paths.append(os.path.join(path, name))

df = pd.DataFrame()
index_levels = ["taxid", "season", "taxclass"]
df.index = pd.MultiIndex(levels=[[]] * len(index_levels), codes=[[]] * len(index_levels), names=index_levels)

for i, file in enumerate(tif_file_paths):
    # print("Reading in files: ", round(i/len(tif_file_paths), 4), end = "\r" )

    path, fname = os.path.split(file)
    taxid = fname.split("-")[-1].split(".")[0]
    season = fname.split("-")[0].split(".")[-1]
    c1 = 0
    for tc in classes:
        if tc in path:
            taxclass = tc
            c1 += 1
    c2 = 0
    for hmap in habmaps.values():
        if hmap in path:
            habmap = hmap
            c2 += 1
    if c1 == 1 and c2 == 1:
        df.loc[(taxid, season, taxclass), habmaps_r[habmap]] = file
df = df.reset_index()
if "historic" not in df.columns:
    df['historic'] = "nan"

filename = os.path.join(args["output_dir"], f"g_file_index_{args['scenario']}_lj.csv")
with open(filename, "w+") as out_file:
    out_file.write("--current_path,--scenario_path,--historic_path,--output_path,--z")
    out_file.write("\n")

    for i, (idx, row) in enumerate(df.iterrows()):
        for z in z_values:
            print(f"Writing littlejohn arguments to {filename}: ", round(i/len(df), 4), end = "\r" )
            curr = row.current
            scen = row.scenario
            hist = row.historic
            ofname = f"Seasonality.{row.season}-{row.taxid}.tif"
            of = os.path.join(args["output_dir"], args["scenario"], str(z), row.taxclass, ofname)

            out_file.write(f"{curr},{scen},{hist},{of},{str(z)}")
            out_file.write("\n")


    