from dataclasses import dataclass
from typing import List, Optional, Any, Tuple

import numpy

from aoh.lib import seasonality
from iucn_modlib.classes.Taxon import Taxon
import iucn_modlib.translator

from layers import Layer, VectorRangeLayer, NullLayer, UniformAreaLayer

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
            print("WARNING: Area map isn't one pixel wide, treating as full layer")
            return Layer.layer_from_file(self.area_map_filename)

class JungModel(LandModel):
    def __init__(self, habitat_map_filename: str, elevation_map_filename: str, area_map_filename: Optional[str] = None):
        super().__init__(habitat_map_filename, elevation_map_filename, area_map_filename, iucn_modlib.translator.toJung)

class ESACCIModel(LandModel):
    def __init__(self, habitat_map_filename: str, elevation_map_filename: str, area_map_filename: Optional[str] = None):
        super().__init__(habitat_map_filename, elevation_map_filename, area_map_filename, iucn_modlib.translator.toESACCI)


def modeller(
    species: Taxon,
    range_path: str,
    land_model: LandModel
) -> List[Tuple]:
    habitatSeasons = seasonality.habitatSeasonality(species)
    rangeSeasons = seasonality.rangeSeasonality(range_path, species.taxonid)
    seasons = list(set(habitatSeasons + rangeSeasons))
    if len(seasons) == 3:
        seasons = ('breeding', 'nonbreeding')
    elif len(seasons) == 2 and 'resident' in seasons:
        seasons = ('breeding', 'nonbreeding')

    elevation_range = (species.elevation_lower, species.elevation_upper)
    habitat_params = iucn_modlib.ModelParameters(
        habMap = None,
        translator = land_model.translator,
        season = ('Resident', 'Seasonal Occurrence Unknown'),
        suitability = ('Suitable', 'Unknown'),
        majorImportance = ('Yes', 'No'),
    )

    # These three map layers don't change across seasons
    habitat_layer = land_model.new_habitat_layer()
    elevation_layer = land_model.new_elevation_layer()
    area_layer = land_model.new_area_layer()

    results = []
    for season in seasons:
        if season == 'resident':
            habitat_params.season = ('Resident', 'Seasonal Occurrence Unknown')
        elif season == 'breeding':
            habitat_params.season = ('Resident', 'Breeding Season', 'Seasonal Occurrence Unknown')
        elif season == 'nonbreeding':
            habitat_params.seasons = ('Resident', 'Non-Breeding Season', 'Seasonal Occurrence Unknown')
        else:
            raise ValueError(f'Unexpected season {season}')
        habitat_list = species.habitatCodes(habitat_params)

        # range layer is only one that is seasonal, so recalculate
        where_filter =  f"id_no = {species.taxonid} and season in ('{season}', 'resident')"
        range_layer = VectorRangeLayer(range_path, where_filter, habitat_layer.pixel_scale, habitat_layer.projection)

        result = _calculate(
            range_layer,
            habitat_layer,
            habitat_list,
            elevation_layer,
            elevation_range,
            area_layer
        )
        results.append([season, result])
    return results


def _calculate(
    range_layer: Layer,
    habitat_layer: Layer,
    habitat_list: List,
    elevation_layer: Layer,
    elevation_range: List,
    area_layer: Layer
) -> None:

    # Work out the intersection of all the maps
    layers = [habitat_layer, elevation_layer, area_layer, range_layer]
    scale = layers[0].pixel_scale
    for layer in layers[1:]:
        if not layer.check_pixel_scale(scale):
            raise ValueError("Not all layers are at the same pixel scale")

    intersection = Layer.find_intersection(layers)
    for layer in layers:
        layer.set_window_for_intersection(intersection)

    STEP = 1

    # all layers now have the same window width/height, so just take the habitat one
    pixel_width = habitat_layer.window.xsize
    pixel_height = habitat_layer.window.ysize

    area_total = 0.0
    for yoffset in range(0, pixel_height, STEP):
        this_step = STEP
        if yoffset + STEP > pixel_height:
            this_step = pixel_height - yoffset

        habitat = habitat_layer.ReadAsArray(0, yoffset, pixel_width, this_step)
        filtered_habitat = numpy.isin(habitat, habitat_list)

        elevation = elevation_layer.ReadAsArray(0, yoffset, pixel_width, this_step)
        filtered_elevation = numpy.logical_and(elevation >= min(elevation_range), elevation <= max(elevation_range))

        species_range = range_layer.ReadAsArray(0, yoffset, pixel_width, this_step)
        pixel_areas = area_layer.ReadAsArray(0, yoffset, pixel_width, this_step)

        # TODO: this isn't free - so if there's no nan's we'd like to avoid this stage
        pixel_areas = numpy.nan_to_num(pixel_areas, copy=False, nan=0.0)

        data = filtered_habitat * filtered_elevation * pixel_areas * species_range
        area_total += numpy.sum(data)

    return area_total
