import itertools
import json
import math
import os
import re
import tempfile
import time
from multiprocessing import Pool, cpu_count

import h3
import numpy as np
from osgeo import ogr, gdal
from yirgacheffe.layers import Layer, DynamicVectorRangeLayer, PixelScale, UniformAreaLayer
from yirgacheffe.window import Area, Window
from yirgacheffe.h3layer import H3CellLayer

CURRENT_RASTERS_DIR = "/maps/results/alison/reptile_current_raster/"
RANGE_FILE = "/maps-priv/maps/ae491/inputs/IUCN/polygons/reptiles_filtered_collected_season.gpkg"
OUTPUT_DIR = "/maps/mwd24/h3results_reptile/"

# This regular expression is how we get the species ID from the filename
FILERE = re.compile('^Seasonality.RESIDENT-(\d+).tif$')

MAG = 7

LONG_BAND_WIDTH = 1.0

def threads():
    return cpu_count()

def geometry_to_pointlist(geo):
    points = []
    for i in range(geo.GetPointCount()):
        p = geo.GetPoint(i)
        points.append((p[1], p[0]))
    return points

def geometry_to_polygons(geo, subdivide=True, prefix=''):
    geotype = geo.GetGeometryType()

    if geotype == ogr.wkbMultiPolygon:
        count = geo.GetGeometryCount()
        polygons = [] # [None] * count
        for i in range(count):
            subgeometry = geo.GetGeometryRef(i)
            subpolys = geometry_to_polygons(subgeometry)
            assert len(subpolys) == 1
            polygon = subpolys[0]

            # envelope is (long_left, long_right, lat_bottom, lat_top)
            envelope = subgeometry.GetEnvelope()
            longitude_width = envelope[1] - envelope[0]
            if (longitude_width < LONG_BAND_WIDTH) or not subdivide:
                polygons.append(polygon)
                continue

            # This poly is quite wide, so split it into smaller chunks
            # OGR is quite slow (relative to the test of the work here)
            # so we just do a simple lat banding
            try:
                slices = []
                for i in range(math.ceil(longitude_width / LONG_BAND_WIDTH)):
                    left = envelope[0] + (i * LONG_BAND_WIDTH)
                    right = envelope[0] + ((i + 1) * LONG_BAND_WIDTH)
                    frame = {
                        'type': 'POLYGON',
                        'coordinates': [
                            [
                                [left, envelope[3]],
                                [right, envelope[3]],
                                [right, envelope[2]],
                                [left, envelope[2]],
                                [left, envelope[3]],
                            ]
                        ]
                    }
                    band_geometry = ogr.CreateGeometryFromJson(json.dumps(frame))
                    if band_geometry is None:
                        raise ValueError("Failed to create mask for slicing")
                    intersection = subgeometry.Intersection(band_geometry)
                    if intersection is None:
                        raise ValueError("Failed to create intersection")
                    if not intersection.IsEmpty():
                        slices.append(intersection)

                for intersection in slices:
                    polygons += geometry_to_polygons(intersection, subdivide=False)
            except ValueError:
                # In rare cases it seems OGR doesn't like the original geometry for
                # creating an intersection. I've seen errors like:
                #
                # ERROR 1: TopologyException: Input geom 0 is invalid: Ring Self-intersection at or near point...
                #
                # and the general advice I've seen is to keep fudging geometries until it
                # works, which isn't a scalable solution. Instead we just take the hit and turn the entire
                # polygon into hextiles in a single pass.
                polygons.append(polygon)

        return polygons

    elif geotype == ogr.wkbPolygon:
        points = []
        for i in range(geo.GetGeometryCount()):
            points.append(geometry_to_pointlist(geo.GetGeometryRef(i)))
        polygon = h3.Polygon(*points)
        return [polygon]

    elif geotype == ogr.wkbGeometryCollection:
        count = geo.GetGeometryCount()
        polygons = []
        for i in range(count):
            polygons += geometry_to_polygons(geo.GetGeometryRef(i), subdivide=False)
        return polygons

    elif geotype == ogr.wkbPoint:
        print(geo)
        return []

    else:
        raise ValueError(f"unknown type {geotype}: {geo.GetGeometryName()}")

def polygon_to_tiles(polygon):

    list_of_tiles = []

    # First we get all the cells with a mid point within the polygon
    tiles = h3.polygon_to_cells(polygon, MAG)
    list_of_tiles.append(tiles)

    # now for every vertice on the polygon, work use the minimum distance path to approximate
    # all cells on the boundry
    polygons = [polygon.outer] + list(polygon.holes)
    for loop in polygons:
        if loop[0] != loop[-1]:
            loop.append(loop[0])
        for i in range(len(loop) - 1):
            start = loop[i]
            end = loop[i + 1]
            start_cell = h3.latlng_to_cell(*start, MAG)
            end_cell = h3.latlng_to_cell(*end, MAG)

            line = [start_cell, end_cell]
            
            if start_cell != end_cell:
                try:
                    distance_estimate = h3.grid_distance(start_cell, end_cell)
                except Exception as e:
                    # if the distance is too far then h3 will give up
                    # this is usually along the boundaries added by 
                    # the chunking we do to let us parallelise things, and so
                    # we don't mi9d, as the polygon_to_cell is sufficient there
                    print(f'Failed to find path from {start} to {end}: {e}')
                    continue

                # In an ideal world we'd use h3.grid_path_cells() for this, but in some places
                # we observe that this does not take the direct route, and the docs do not
                # say that it'll produce an optimal output, nor that the results are stable.
                # Instead we do this appoximation by hand, which isn't guaranteed to generate
                # a contiguous line of cells, but is good enough, particularly once we add
                # cell padding, which we did anyway even on the original implementation that
                # had h3.grid_path_cells()
                if distance_estimate:
                    diffs = ((end[0] - start[0]) / float(distance_estimate), (end[1] - start[1]) / float(distance_estimate))
                    for i in range(distance_estimate):
                        here = (start[0] + (diffs[0] * i), start[1] + (diffs[1] * i))
                        cell = h3.latlng_to_cell(*here, MAG)
                        assert h3.is_valid_cell(cell)
                        line.append(cell)
                else:
                    line = h3.grid_path_cells(
                        h3.latlng_to_cell(*start, MAG),
                        h3.latlng_to_cell(*end, MAG)
                    )

            list_of_tiles.append(line)
            for cell in line:
                list_of_tiles.append(h3.grid_disk(cell, 3))


    tiles = itertools.chain.from_iterable(list_of_tiles)

    return tiles

def process_cell(args):
    aoh_layer_path, tile = args

    # Load the raster of total aoh of species
    aoh_layer_path = os.path.join(CURRENT_RASTERS_DIR, f'Seasonality.RESIDENT-{species_id}.tif')
    aoh_layer = Layer.layer_from_file(aoh_layer_path)

    # create a layer the H3 cell of interest
    tile_layer = H3CellLayer(tile, aoh_layer.pixel_scale, aoh_layer.projection)

    # calculate intersection
    layers = [aoh_layer, tile_layer]
    try:
        intersection = Layer.find_intersection(layers)
    except ValueError:
        return (tile, 0.0)
    for layer in layers:
        try:
            layer.set_window_for_intersection(intersection)
        except:
            print(f'Failed to intersect {tile} with for {layer} with area {layer.area} and {intersection}')
            raise

    # work out area of habitate contributed by just that cell
    calc = aoh_layer * tile_layer
    try:
        tile_aoh = calc.sum()
    except:
        print(f' Failed to process {tile} with {intersection} at scale {aoh_layer.pixel_scale}')
        raise

    return (tile, tile_aoh)


def tiles_to_area(aoh_layer_path, species_id, tiles, s2):
    # we now have all the tiles, so work out the AoH in just that tile
    results = []
    args = [(aoh_layer_path, tile) for tile in tiles]

    with Pool(processes=threads()) as p:
        results = p.map(process_cell, args)

    s3 = time.time()
    print(f"Processed {len(tiles)} tiles in {s3 - s2} seconds - {float(len(tiles)) / (s3 - s2)} tiles per second")

    total = 0.0
    max = 0.0
    with open(os.path.join(OUTPUT_DIR, f'res_{species_id}_{MAG}.csv'), 'w') as f:
        f.write('cell, area\n')
        for result in results:
            total += result[1]
            if result[1] > max:
                max = result[1]
            f.write(f'{result[0]}, {result[1]},\n')

    end = time.time()
    return total


def get_original_aoh_info(raster_path: str):
    aoh_layer = Layer.layer_from_file(aoh_layer_path)
    return aoh_layer.sum()


def get_range_polygons(range_path, species_id):
    where_filter = f"id_no = {species_id} and season = 'resident'"

    # The pixel scale and projection don't matter here, as we're just 
    # abusing yirgacheffe to load the range file. Feels like I need to split this
    # out at some point
    layer = DynamicVectorRangeLayer(range_path, where_filter, PixelScale(0.1, 0.1), "UNUSED")
    range_layer = layer.range_layer
    range_layer.ResetReading()
    polygons = []
    feature = range_layer.GetNextFeature()
    while feature:
        geo = feature.GetGeometryRef()
        polygons.append(geometry_to_polygons(geo, subdivide=True))
        feature = range_layer.GetNextFeature()
    return list(itertools.chain.from_iterable(polygons))
    

if __name__ == "__main__":

    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    except FileExistsError:
        print(f'Could not create {OUTPUT_DIR} as file is there')
        sys.exit(1)
    except PermissionError:
        print(f'Could not create {OUTPUT_DIR} due to permissions')
        sys.exit(1)

    species_list = [FILERE.match(x).groups()[0] for x in os.listdir(CURRENT_RASTERS_DIR) if FILERE.match(x)]
    species_list.sort()

    # for test run, just do first dozen
    for species_id in species_list:
        print(species_id)

        # Due to errors as we find new corner cases, we keep having to restart the script
        # so we don't overwrite old results and just keep moving on. 
        if os.path.exists(os.path.join(OUTPUT_DIR, f'res_{species_id}_{MAG}.csv')):
            print('Species result exists, skipping')
            continue

        start = time.time()
        aoh_layer_path = os.path.join(CURRENT_RASTERS_DIR, f'Seasonality.RESIDENT-{species_id}.tif')

        # We can't currently parallelise either of these tasks, but they are independant, so we can 
        # at least run them concurrently...
        try:
            with Pool(processes=threads()) as p:
                res_aoh_total = p.apply_async(get_original_aoh_info, (aoh_layer_path,))
                res_polygons = p.apply_async(get_range_polygons, (RANGE_FILE, species_id))

                aoh_layer_total = res_aoh_total.get()
                polygons = res_polygons.get()
        except (FileNotFoundError, TypeError):
            print(f'Failed to load raster for {species_id}, skipping')
            continue

        if aoh_layer_total == 0.0:
            print(f'Skipping species, as AoH is {aoh_layer_total}')
            continue

        s1 = time.time()
        print(f"Found {len(polygons)} polygons in {s1 - start} seconds")

        # The h3 lookup can be ran concurrently thought
        tiles = set()
        with Pool(processes=threads()) as p:
            results = p.map(polygon_to_tiles, polygons)
            tiles = set(itertools.chain.from_iterable(results))

        s2 = time.time()
        print(f"Found {len(tiles)} tiles in {s2 - s1} seconds")

        total = tiles_to_area(aoh_layer_path, species_id, tiles, s2)
        diff = ((total - aoh_layer_total) / aoh_layer_total) * 100.0
        if f'{abs(diff):.5f}' != '0.00000':
            print(f'AoH layer total: {aoh_layer_total}')
            print(f'Hex tile total:  {total}')
            print(f'Error is {diff:.5f}%')

        end = time.time()
        print(f'{species_id} at mag {MAG} took {end - start} seconds')
