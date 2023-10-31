# The shapefile's column for season is coded into integers from 1-5:
#     1 = Resident,
#     2 = Breeding,
#     3 = Non-Breeding,
#     4 = Passing,
#     5 = Unknown
# We don't care about 4 and 5, so they just get turned into 1s.
# BUT we need these to be in text format, so we can merge the csv file with the shape file on this column (as well as
# internalTaxonId) HOWEVER in the case of Amphibians, although the csv file seems to indicate that some species have
# breeding grounds as well as residency, it is actually only the HABITAT not the LOCATION that changes for breeding:
# e.g. frogs will live in the forest, but breed in the ponds, but stay in the same geographical area. Therefore if
# every species has a 1, then I will merge the tables on internaltaxonId only, and copy the shapefiles for both
# breeding and non-breeding/resident.

# Imports
import glob
import json
import os.path
import sys
from typing import Tuple

import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon

PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT = 10

# This function opens up and reads the files necessary to make the geopackage from the specified folder
def reading_files(folder: str) -> Tuple[pd.DataFrame, pd.DataFrame, gpd.GeoDataFrame]:
    print("Reading csv files...")
    habitats = pd.read_csv(os.path.join(folder, "habitats.csv"))
    allotherfields = pd.read_csv(os.path.join(folder, "all_other_fields.csv"))
    print("Reading shape file...")
    shapename = ''.join(glob.glob(os.path.join(folder, '*.shp')))
    geo = gpd.read_file(shapename)
    return habitats, allotherfields, geo

# The shapefile's column for season is coded into integers from 1-5:
# 1 = Resident, 2 = Breeding, 3 = Non-Breeding, 4 = Passing, 5 = Unknown
# The habitats.csv column for season is the string version of the above coding.
# Any season that is passing, unknown, or simply doesn't have a value, is marked as resident
def changing_seasons(seasons: pd.Series) -> pd.Series:
    if isinstance(seasons, str):
        seasons.fillna("Resident", inplace = True)
    season_array = []
    for season in seasons:
        if ("NON-BREEDING" in str(season).upper()) or ('3' in str(season)):
            season_array.append(3)
        elif ("BREEDING" in str(season).upper()) or ('2' in str(season)):
            season_array.append(2)
        else:
            season_array.append(1)
    return pd.Series(season_array)

#This function extracts, fills and replaces key pandas Series
#This function also creates temporary pandas DataFrames for manipulation
def extracting_series(
    habitats: pd.DataFrame,
    allotherfields: pd.DataFrame,
    geo: gpd.GeoDataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, gpd.GeoDataFrame]:
    print("Extracting key data...")
    habitats['majorImportance'].fillna("Yes", inplace = True)

    temp_season = changing_seasons(habitats['season'])
    habitats = habitats.drop(['season'], axis = 1)
    habitats = habitats.assign(season = temp_season)

    temp = pd.DataFrame(data = pd.Series(allotherfields['internalTaxonId']))
    temp = temp.assign(ElevationLower = pd.Series(allotherfields['ElevationLower.limit']).fillna(-500.0),
                       ElevationUpper = pd.Series(allotherfields['ElevationUpper.limit']).fillna(9000.0))

    notfinal = gpd.GeoDataFrame(data = geo['geometry'])
    # We've seen both CAPS and noncaps column names, so here we just force everything to CAPS for consistency
    geo = geo.rename(columns={
        'id_no': 'ID_NO',
        'presence': 'PRESENCE',
        'origin': 'ORIGIN',
        'seasonal': 'SEASONAL',
    })
    notfinal = notfinal.assign(internalTaxonId = pd.Series(geo['ID_NO']), Presence = pd.Series(geo['PRESENCE']),
                               Origin = pd.Series(geo['ORIGIN']), season = changing_seasons(pd.Series(geo['SEASONAL'])))

    return habitats, temp, notfinal

# This function aggregates the data from the two csv files together
def habitats_sort(habitats: pd.DataFrame, temp: pd.DataFrame) -> pd.DataFrame:
    print("Grouping files...")
    habitats = habitats.groupby(['internalTaxonId', 'season']).agg({
        'code': lambda x: json.dumps(x.tolist()),
        'majorImportance': lambda x: json.dumps(x.tolist()),
        'suitability': lambda x: json.dumps(x.tolist())
    }).reset_index()
    habitats = habitats.merge(temp, how='left', on='internalTaxonId')
    return habitats

# This function takes the 'geometry' row and if the row has more than one polygon or
# multipolygon in it, combines them together to make a new multipolygon
def to_polygons(geometries):
    for geometry in geometries:
        if isinstance(geometry, Polygon):
            yield geometry
        elif isinstance(geometry, MultiPolygon):
            yield from geometry.geoms
        else:
            raise TypeError(f"Unexpected type: {type(geometry)}")

# This function aggregates the GeoDataFrame, and then merges it with the data from the csv files
def shape_sort(notfinal: gpd.GeoDataFrame, habitats: pd.DataFrame) -> gpd.GeoDataFrame:
    #print(notfinal) #Debugging
    print("Combining files...")
    notfinal = (notfinal.groupby(['internalTaxonId', 'season']).agg({
        'Presence': lambda x: json.dumps(x.tolist()),
        'Origin': lambda x: json.dumps(x.tolist()),
        'geometry': lambda x: MultiPolygon(to_polygons(x))
    })).reset_index()
    notfinal = notfinal.merge(habitats, how='left', on=['internalTaxonId', 'season'])
    return notfinal

# This function converts the GeoDataFrame into a GeoPackage
def to_file(notfinal: gpd.GeoDataFrame, folder: str) -> None:
    print("Building file...")
    # When the non-geometry related series are added, final becomes a DataFrame -
    # so it has to be turned back into a GeoDataFrame
    final = gpd.GeoDataFrame(notfinal, geometry = 'geometry')
    final.to_file(os.path.join(folder, "keydata.gpkg"), driver = 'GPKG', index = None)
    print("File keydata.gpkg created")

# This function records the IUCN Red List Taxonomy ID for the species that were
# not in the final geopackage
def unrecorded_data(habitats: pd.DataFrame, notfinal: gpd.GeoDataFrame):
    taxid = pd.DataFrame(habitats['internalTaxonId'])
    csv_set = set()
    for _, row1 in taxid.iterrows():
        csv_set.add(row1['internalTaxonId'])
    nopair = set()
    inopair = 0
    for _, row2 in notfinal.iterrows():
        if row2['internalTaxonId'] in csv_set:
            continue
        nopair.add(row2['internalTaxonId'])
        inopair = inopair+1
    if inopair > 0:
        print("There was not enough data for: ")
        for x in nopair:
            print(x)

def main():
    # Setting up the keydata.csv file in the correct folder to write to.
    if len(sys.argv) == 1:
        folder = input("Enter folder of csv files: ")
    else:
        folder = sys.argv[1]
    habitats, allotherfields, geo = reading_files(folder)
    habitats, temp, notfinal = extracting_series(habitats, allotherfields, geo)
    habitats = habitats_sort(habitats, temp)
    notfinal = shape_sort(notfinal, habitats)
    to_file(notfinal, folder)
    #unrecorded_data(habitats, notfinal)

if __name__ == "__main__":
    main()
