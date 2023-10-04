"""
Generate list of args for use with littlejohn and global_code_residents_pixel.py

Note this works for my file structure specifically to avoid a crazy amount of 
messy kwargs, but can be adapted fairly easily.

"""

import os
import argparse
import pandas as pd
import warnings

classes = ["birds", "mammals", "amphibians", "reptiles"]
season = "RESIDENT"
habmaps = {"historic"   : "pnv",
           "scenario"   : "restore",
           "current"    : "current_L1L2"
           }
habmaps_r = {v: k for k, v in habmaps.items()}

def splitall(path):
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts

parser = argparse.ArgumentParser(description="")
parser.add_argument('--target_dir',
        type=str,help="Look for folders called 'search' in this directory",
        required=True,dest="target_dir")
parser.add_argument('--output_dir',
                    type = str, help = "where to save the csv",
                    required = True, dest = "output_dir")
args = vars(parser.parse_args())


for c in classes:
    os.makedirs(os.path.join(args["output_dir"], c), exist_ok = True)

f = []
for path, subdirs, files in os.walk(args["target_dir"]):
    for name in files:
        f.append(os.path.join(path, name))
f = [file for file in f if ".csv" in file]

df = pd.DataFrame()
index_levels = ["taxid", "season", "taxclass"]
df.index = pd.MultiIndex(levels=[[]] * len(index_levels), codes=[[]] * len(index_levels), names=index_levels)

for i, file in enumerate(f):
    print("Reading in files: ", round(i/len(f), 4), end = "\r" )
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
print(df)
with open(os.path.join(args["output_dir"], "file_index_lj.csv"), "w+") as out_file:

    out_file.write("--current_path,--scenario_path,--historic_path,--output_path")
    out_file.write("\n")

    for i, (idx, row) in enumerate(df.iterrows()):
        print(f"Writing littlejohn arguments to {os.path.join(args['output_dir'], 'file_index_lj.csv')}: ", round(i/len(df), 4), end = "\r" )
        curr = row.current
        scen = row.scenario
        hist = row.historic
        ofname = f"Seasonality.{row.season}-{row.taxid}.csv"
        of = os.path.join(args["output_dir"], row.taxclass, ofname)

        out_file.write(f"{curr},{scen},{hist},{of}")
        out_file.write("\n")


    