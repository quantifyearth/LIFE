from dataclasses import dataclass
from typing import List, Optional, Any

import numpy

import iucn_modlib.translator

from layers import Layer, VectorMaskLayer, NullLayer, UniformAreaLayer

@dataclass
class LandModel:
    landc: str
    dem: str
    area: Optional[str]
    translator: Any

class JungModel(LandModel):
    def __init__(self, landc: str, dem: str, area: Optional[str] = None):
        super().__init__(landc, dem, area, iucn_modlib.translator.toJung)

class ESACCIModel(LandModel):
    def __init__(self, landc: str, dem: str, area: Optional[str] = None):
        super().__init__(landc, dem, area, iucn_modlib.translator.toESACCI)


def modeller(
    vector_mask_filename: str,
    mask_filter: str,
    habitat_map_filename: str,
    habitat_list: List,
    elevation_map_filename: str,
    elevation_range: List,
    area_map_filename: Optional[str],
) -> None:

    habitat_layer = Layer.layer_from_file(habitat_map_filename)
    elevation_layer = Layer.layer_from_file(elevation_map_filename)
    if area_map_filename is not None:
        try:
            area_layer = UniformAreaLayer.layer_from_file(area_map_filename)
        except ValueError:
            print("WARNING: Area map isn't one pixel wide, treating as full layer")
            area_layer = Layer.layer_from_file(area_map_filename)
    else:
        area_layer = NullLayer()

    mask_layer = VectorMaskLayer(vector_mask_filename, mask_filter, habitat_layer.pixel_scale, habitat_layer.projection)  

    # Work out the intersection of all the maps
    layers = [habitat_layer, elevation_layer, area_layer, mask_layer]
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

        mask = mask_layer.ReadAsArray(0, yoffset, pixel_width, this_step)
        pixel_areas = area_layer.ReadAsArray(0, yoffset, pixel_width, this_step)

        # TODO: this isn't free - so if there's no nan's we'd like to avoid this stage
        pixel_areas = numpy.nan_to_num(pixel_areas, copy=False, nan=0.0)

        data = filtered_habitat * filtered_elevation * pixel_areas * mask 
        area_total += numpy.sum(data)

    return area_total
