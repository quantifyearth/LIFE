import itertools
import json
import math
import os
from multiprocessing import Pool, cpu_count
import time

import h3
import numpy as np
from osgeo import ogr
from yirgacheffe.layers import Layer, DynamicVectorRangeLayer, PixelScale, UniformAreaLayer
from yirgacheffe.h3layer import H3CellLayer


# CURRENT_RASTERS_DIR = "/maps/results/alison/mammal_examples/current"
CURRENT_RASTERS_DIR = "/maps/results/alison/mammal_current_raster/"
AOH_VALUES_CSV = "/home/ae491/dev/persistence-calculator/mammal_example_P.csv"
RANGE_FILE = "/maps/biodiversity/mammals_extinct_final.gpkg"

#  [17975, 15951, 19353, 29673, 41686]
ID_LIST = [15251, ]#17975, 15951, 19353, 29673, 41686]

MAG = 8

LONG_BAND_WIDTH = 2.0

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
            
            for i in range(math.ceil(longitude_width / LONG_BAND_WIDTH)):
                left = envelope[0] + (i * LONG_BAND_WIDTH)
                right = envelope[0] + ((i + 1) * LONG_BAND_WIDTH)
                frame = {
                    'type': 'POLYGON',
                    'coordinates': [
                        [
                            [left, 90.0],
                            [right, 90.0],
                            [right, -90.0],
                            [left, -90.0],
                            [left, 90.0],
                        ]
                        # [
                        #     [90.0, left],
                        #     [90.0, right],
                        #     [-90.0, right],
                        #     [-90.0, left],
                        # ]
                    ]
                }
                band_geometry = ogr.CreateGeometryFromJson(json.dumps(frame))
                assert band_geometry
                intersection = subgeometry.Intersection(band_geometry)
                assert intersection
                if not intersection.IsEmpty():
                    polygons += geometry_to_polygons(intersection, subdivide=False)


            # # Some of these can be very big, so first do a quick check at half the resolution, and 
            # # and then decompose.
            # course_tiles = h3.polygon_to_cells(polygon, math.floor(1))
            # # print(len(course_tiles))
            # if (len(course_tiles) < 1) or not subdivide:
            #     polygons.append(polygon)
            #     continue

            # processed_tiles = set()
            # next_round = set(course_tiles)
            # while next_round:
            #     this_round = next_round
            #     next_round = set()

            #     for tile in this_round:
            #         tile_boundary = h3.cell_to_boundary(tile, geo_json=True)
            #         geojson = {
            #             'type': 'POLYGON',
            #             'coordinates': [tile_boundary],
            #         }
            #         tile_geometry = ogr.CreateGeometryFromJson(json.dumps(geojson))

            #         intersection = geo.Intersection(tile_geometry)
            #         if not intersection.IsEmpty():
            #             _ = geometry_to_polygons(intersection, subdivide=False)
            #             # polygons += geometry_to_polygons(intersection, subdivide=False)
            #             processed_tiles.add(tile)
            #             next_round = next_round.union(h3.grid_ring(tile, 1))

            #     next_round = next_round - processed_tiles

        return polygons

    elif geotype == ogr.wkbPolygon:
        points = []
        for i in range(geo.GetGeometryCount()):
            points.append(geometry_to_pointlist(geo.GetGeometryRef(i)))
        polygon = h3.Polygon(*points)
        return [polygon]

    else:
        return []

def polygon_to_tiles(polygon):
    tiles = set(h3.polygon_to_cells(polygon, MAG))
    outer = {h3.latlng_to_cell(*x, MAG) for x in polygon.outer}
    tiles = tiles.union(outer)
    for hole in polygon.holes:
        boundary = {h3.latlng_to_cell(*x, MAG) for x in hole}
        tiles = tiles.union(boundary)
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
    intersection = Layer.find_intersection(layers)
    for layer in layers:
        layer.set_window_for_intersection(intersection)

    # work out area of habitate contributed by just that cell
    calc = aoh_layer * tile_layer
    tile_aoh = calc.sum()
    
    return (tile, tile_aoh)

def process_pure_cell(args):
    aoh_layer_path, tile = args

    # Load the raster of total aoh of species
    aoh_layer_path = os.path.join(CURRENT_RASTERS_DIR, f'Seasonality.RESIDENT-{species_id}.tif')
    aoh_layer = Layer.layer_from_file(aoh_layer_path)
    area_layer = UniformAreaLayer.layer_from_file('/maps-priv/maps/biodiversity/jung_aoh_basemaps/small_jung.tif')

    # create a layer the H3 cell of interest
    tile_layer = H3CellLayer(tile, aoh_layer.pixel_scale, aoh_layer.projection)

    # calculate intersection
    layers = [aoh_layer, tile_layer]
    intersection = Layer.find_intersection(layers)
    layers.append(area_layer)
    for layer in layers:
        layer.set_window_for_intersection(intersection)

    # work out area of habitate contributed by just that cell
    calc = area_layer * tile_layer
    tile_aoh = calc.sum()
    
    return (tile, tile_aoh)

for species_id in ID_LIST:
    print(species_id)
    start = time.time()
    aoh_layer_path = os.path.join(CURRENT_RASTERS_DIR, f'Seasonality.RESIDENT-{species_id}.tif')
    aoh_layer = Layer.layer_from_file(aoh_layer_path)

    where_filter = f"id_no = {species_id} and season = 'resident'"
    layer = DynamicVectorRangeLayer(RANGE_FILE, where_filter, aoh_layer.pixel_scale, aoh_layer.projection)
    range_layer = layer.range_layer
    range_layer.ResetReading()

    # Get the polygons from OGR - this can not be done concurrently, as osgeo and multiprocessing
    # are not friends
    polygons = []
    feature = range_layer.GetNextFeature()
    while feature:
        geo = feature.GetGeometryRef()
        polygons += geometry_to_polygons(geo, subdivide=True)
        feature = range_layer.GetNextFeature()
    s1 = time.time()
    print(f"Found {len(polygons)} polygons in {s1 - start} seconds")
    

    # The h3 lookup can be ran concurrently thought
    tiles = set()
    with Pool(cpu_count()) as p:
        results = p.map(polygon_to_tiles, polygons)
        tiles = set(itertools.chain.from_iterable(results))

    s2 = time.time()
    print(f"Found {len(tiles)} tiles in {s2 - s1} seconds")

    # we now have all the tiles, so work out the AoH in just that tile
    results = []
    args = [(aoh_layer_path, tile) for tile in tiles]

    with Pool(cpu_count()) as p:
        results = p.map(process_cell, args)
    # results += res

    s3 = time.time()
    print(f"Processed {len(tiles)} tiles in {s3 - s2} seconds - {float(len(tiles)) / (s3 - s2)} tiles per second")

    with open(f'res_{species_id}_{MAG}.csv', 'w') as f:
        for result in results:
            f.write(f'{result[0]}, {result[1]},\n')

    end = time.time()
    print(f'Wrote out results in {end - s3} seconds')
    print(f'{species_id} at mag {MAG} took {end - start} seconds')

