import os
import shutil
import tempfile
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Any, Tuple, Set

import numpy
from osgeo import gdal

from iucn_modlib.classes.Taxon import Taxon
import iucn_modlib.translator

from .layers import Layer, VectorRangeLayer, NullLayer, UniformAreaLayer

@dataclass
class LandModel:
    habitat_map_filename: str
    elevation_map_filename: str
    area_map_filename: Optional[str]
    translator: Any

    def new_habitat_layer(self) -> Layer:
        return Layer.layer_from_file(self.habitat_map_filename)

    def new_elevation_layer(self) -> Layer:
        return Layer.layer_from_file(self.elevation_map_filename)

    def new_area_layer(self) -> Layer:
        if self.area_map_filename is None:
            return NullLayer()
        try:
            return UniformAreaLayer.layer_from_file(self.area_map_filename)
        except ValueError:
            return Layer.layer_from_file(self.area_map_filename)

class JungModel(LandModel):
    def __init__(self, habitat_map_filename: str, elevation_map_filename: str, area_map_filename: Optional[str] = None):
        super().__init__(habitat_map_filename, elevation_map_filename, area_map_filename, iucn_modlib.translator.toJung)

class ESACCIModel(LandModel):
    def __init__(self, habitat_map_filename: str, elevation_map_filename: str, area_map_filename: Optional[str] = None):
        super().__init__(habitat_map_filename, elevation_map_filename, area_map_filename,
            iucn_modlib.translator.toESACCI)


class Seasonality(Enum):
    RESIDENT = "resident"
    BREEDING = "breeding"
    NONBREEDING = "nonbreeding"

    @property
    def iucn_seasons(self) -> Tuple:
        if self.value == 'resident':
            return ('Resident', 'Seasonal Occurrence Unknown')
        elif self.value == 'breeding':
            return ('Resident', 'Breeding Season', 'Seasonal Occurrence Unknown')
        elif self.value == 'nonbreeding':
            return ('Resident', 'Non-Breeding Season', 'Seasonal Occurrence Unknown')


def calculator(
    species: Taxon,
    range_path: str,
    land_model: LandModel,
    seasonality: Seasonality,
    results_path: Optional[str]
) -> List[Tuple[str, float, str]]:

    habitat_params = iucn_modlib.ModelParameters(
        habMap = None,
        translator = land_model.translator,
        season = seasonality.iucn_seasons,
        suitability = ('Suitable', 'Unknown'),
        majorImportance = ('Yes', 'No'),
    )
    habitat_list = species.habitatCodes(habitat_params)

    # These three map layers don't change across seasons
    habitat_layer = land_model.new_habitat_layer()
    elevation_layer = land_model.new_elevation_layer()
    area_layer = land_model.new_area_layer()

    # range layer is only one that is seasonal, so recalculate
    where_filter =  f"id_no = {species.taxonid} and season in ('{seasonality.value}', 'resident')"
    pixel_scale = habitat_layer.pixel_scale
    assert pixel_scale
    try:
        range_layer = VectorRangeLayer(range_path, where_filter, pixel_scale, habitat_layer.projection)
    except ValueError:
        return 0.0, None

    # Work out the intersection of all the maps
    layers = [habitat_layer, elevation_layer, area_layer, range_layer]
    intersection = Layer.find_intersection(layers)
    for layer in layers:
        layer.set_window_for_intersection(intersection)

    with tempfile.TemporaryDirectory() as tempdir:
        results_dataset = None
        results_dataset_filename = ''
        if results_path:
            results_dataset_filename = f'{season}-{species.taxonid}.tif'
            results_dataset = gdal.GetDriverByName('GTiff').Create(
                os.path.join(tempdir, results_dataset_filename),
                habitat_layer.window.xsize,
                habitat_layer.window.ysize,
                1,
                gdal.GDT_Float32, # TODO: This needs to vary based on area optionality
                ['COMPRESS=LZW']
            )
            results_dataset.SetProjection(habitat_layer.projection)
            results_dataset.SetGeoTransform(habitat_layer.geo_transform)

        result = _calculate(
            range_layer,
            habitat_layer,
            habitat_list,
            elevation_layer,
            (species.elevation_lower, species.elevation_upper),
            area_layer,
            results_dataset.GetRasterBand(1) if results_dataset else None
        )
        # if we got here, then consider the experiment a success
        if results_dataset:
            del results_dataset # aka close for gdal
            shutil.move(os.path.join(tempdir, results_dataset_filename),
                os.path.join(results_path, results_dataset_filename))
        return result, results_dataset_filename


def _calculate(
    range_layer: Layer,
    habitat_layer: Layer,
    habitat_list: List,
    elevation_layer: Layer,
    elevation_range: Tuple[float, float],
    area_layer: Layer,
    results_dataset: Optional[gdal.Band]
) -> float:

    ystep = 1

    # all layers now have the same window width/height, so just take the habitat one
    pixel_width = habitat_layer.window.xsize
    pixel_height = habitat_layer.window.ysize

    area_total = 0.0
    for yoffset in range(0, pixel_height, ystep):
        this_step = ystep
        if yoffset + this_step > pixel_height:
            this_step = pixel_height - yoffset

        habitat, elevation, species_range, pixel_areas = [
            x.read_array(0, yoffset, pixel_width, this_step)
            for x in [habitat_layer, elevation_layer, range_layer, area_layer]
        ]

        filtered_habitat = numpy.isin(habitat, habitat_list)
        filtered_elevation = numpy.logical_and(elevation >= min(elevation_range), elevation <= max(elevation_range))

        # TODO: this isn't free - so if there's no nan's we'd like to avoid this stage
        pixel_areas = numpy.nan_to_num(pixel_areas, copy=False, nan=0.0)

        data = filtered_habitat * filtered_elevation * pixel_areas * species_range
        if results_dataset:
            results_dataset.WriteArray(data, 0, yoffset)
        area_total += numpy.sum(data)

    return area_total
