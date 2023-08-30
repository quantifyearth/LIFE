#!/usr/bin/env python3

import os
import sys

from osgeo import gdal
import pandas as pd

from yirgacheffe import WSG_84_PROJECTION
from yirgacheffe.window import Area, PixelScale
from yirgacheffe.layers import RasterLayer, H3CellLayer, YirgacheffeLayer

def main() -> None:
    if len(sys.argv) != 3:
        print(f'USAGE: {sys.argv[0]} CSV TIF')
        sys.exit(-1)
    filename = sys.argv[1]

    # Make up the geo transform based on image resolution
    width, height = 3840.0, 2180.0 # 4K screen
    scale = PixelScale(360.0 / width, -180.0/height)
    area = Area(left=-180.0, right=180, top=90, bottom=-90)

    ext = os.path.splitext(filename)[1]
    if ext == '.parquet':
        tiles_df = pd.read_parquet(filename)
    elif ext == '.csv':
        tiles_df = pd.read_csv(filename, index_col=False)
    elif ext == '.hdf5':
        tiles_df = pd.read_hdf(filename)
    else:
        print(f'unrecognised data type {ext}')
        sys.exit(-1)

    # Every time you write to a gdal layer that has a file store you
    # risk it trying to save the compressed file, which is slow. So
    # we first use a memory only raster layer, and then at the end save
    # the result we built up out to file.
    scratch = RasterLayer.empty_raster_layer(area, scale, gdal.GDT_Float64)

    for _, tile, area in tiles_df.itertuples():
        if area == 0.0:
            continue
        try:
            tile_layer = H3CellLayer(tile, scale, WSG_84_PROJECTION)
        except ValueError:
            print(f"Skipping tile with invalid id: {tile}")
            continue

        scratch.reset_window()
        layers = [scratch, tile_layer, scratch]
        intersection = YirgacheffeLayer.find_intersection(layers)
        for layer in layers:
            layer.set_window_for_intersection(intersection)

        result = scratch + (tile_layer * area)
        result.save(scratch)

    # now we've done the calc in memory, save it to a file
    scratch.reset_window()
    output = RasterLayer.empty_raster_layer_like(scratch, filename=sys.argv[2])
    scratch.save(output)

if __name__ == "__main__":
    main()
