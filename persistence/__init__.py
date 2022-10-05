# AoH calculator code for the 4C persistence calculator, a more specialised
# version of the logic from working on Daniele Baisero's AoH library.
#
# There's two seperate versions of the actual calculation - one for CPU use
# one for use with CUDA GPUs. Originally I wanted to have one function with
# conditional bits of code, but almost all the code ended up being conditional
# one way or the other, so the logic was hard to read. So instead we now have
# some duplication, but it is easier to see the logic in each one.

import os
import shutil
import tempfile
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Any, Tuple, Set

import numpy
from osgeo import gdal
try:
    import cupy
    import cupyx
    USE_GPU = True
except ModuleNotFoundError:
    USE_GPU = False

from iucn_modlib.classes.Taxon import Taxon
from iucn_modlib.classes.HabitatFilters import HabitatFilters
import iucn_modlib.translator

from yirgacheffe.layers import Layer, DynamicVectorRangeLayer, NullLayer, UniformAreaLayer

# When working with rasters we read larger chunks that just a single line, despite that usually
# being what GDAL recommends if you ask for the efficient block size for larger files. There's
# two reasons for this:
# 1: We use DynamicVectorRangeLayer to incrementally rasterize the vector habitat maps, so as to
#    not need to hold the entire raster in memory at once. Doing that on a per line basis is
#    somewhat slow. Thus the step is a tradeoff between memory allocation and CPU cost of
#    processing the vectors. Moving from 1 line to 512 lines cut the runtime by close to half for
#    the small sample I tested.
# 2: With the CUDA version of the calculator you have a cost of moving the data from main memory
#    over to GPU memory and back. Again, doing so on a line by line basis is inefficient, and using
#    a larger chunk size gives us better efficiency.
YSTEP = 512


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
        else:
            raise NotImplementedError(f'Unhandled seasonlity value {self.value}')


def calculator(
    species: Taxon,
    range_path: str,
    land_model: LandModel,
    seasonality: Seasonality,
    results_path: Optional[str]
) -> Tuple[float, Optional[str]]:

    # We do not re-use data in this, so set a small block cache size for GDAL, otherwise
    # it pointlessly hogs memory, and then spends a long time tidying it up after.
    gdal.SetCacheMax(1024 * 1024 * 16)

    habitat_params = iucn_modlib.HabitatFilters(
        season = seasonality.iucn_seasons,
        suitability = ('Suitable', 'Unknown'),
        majorImportance = ('Yes', 'No'),
    )
    habitat_list = land_model.translator(species.habitatCodes(habitat_params))

    # These three map layers don't change across seasons
    habitat_layer = land_model.new_habitat_layer()
    elevation_layer = land_model.new_elevation_layer()
    area_layer = land_model.new_area_layer()

    # range layer is only one that is seasonal, so recalculate
    where_filter =  f"id_no = {species.taxonid} and season in ('{seasonality.value}', 'resident')"
    pixel_scale = habitat_layer.pixel_scale
    assert pixel_scale
    try:
        range_layer = DynamicVectorRangeLayer(range_path, where_filter, pixel_scale, habitat_layer.projection)
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
            results_dataset_filename = f'{seasonality}-{species.taxonid}.tif'
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

        calculate_function = _calculate_cpu if not USE_GPU else _calculate_cuda

        result = calculate_function(
            range_layer,
            habitat_layer,
            habitat_list,
            elevation_layer,
            (species.elevation_lower, species.elevation_upper),
            area_layer,
            results_dataset.GetRasterBand(1) if results_dataset else None
        )
        # if we got here, then consider the experiment a success
        if results_dataset and results_path:
            del results_dataset # aka close for gdal
            shutil.move(os.path.join(tempdir, results_dataset_filename),
                os.path.join(results_path, results_dataset_filename))
        return result, results_dataset_filename


def _calculate_cpu(
    range_layer: Layer,
    habitat_layer: Layer,
    habitat_list: List,
    elevation_layer: Layer,
    elevation_range: Tuple[float, float],
    area_layer: Layer,
    results_dataset: Optional[gdal.Band]
) -> float:

    # all layers now have the same window width/height, so just take the habitat one
    pixel_width = habitat_layer.window.xsize
    pixel_height = habitat_layer.window.ysize

    area_total = 0.0
    for yoffset in range(0, pixel_height, YSTEP):
        this_step = YSTEP
        if yoffset + this_step > pixel_height:
            this_step = pixel_height - yoffset

        habitat, elevation, species_range, pixel_areas = [
            x.read_array(0, yoffset, pixel_width, this_step)
            for x in [habitat_layer, elevation_layer, range_layer, area_layer]
        ]

        filtered_habitat = numpy.isin(habitat, habitat_list)
        filtered_elevation = numpy.logical_and(elevation >= min(elevation_range), elevation <= max(elevation_range))
        del habitat
        del elevation

        # TODO: this isn't free - so if there's no nan's we'd like to avoid this stage
        pixel_areas = numpy.nan_to_num(pixel_areas, copy=False, nan=0.0)

        data = filtered_habitat * filtered_elevation * pixel_areas * species_range
        if results_dataset:
            results_dataset.WriteArray(data, 0, yoffset)
        area_total += numpy.sum(data)

        del filtered_habitat
        del filtered_elevation
        del species_range

    return area_total


def _calculate_cuda(
    range_layer: Layer,
    habitat_layer: Layer,
    habitat_list: List,
    elevation_layer: Layer,
    elevation_range: Tuple[float, float],
    area_layer: Layer,
    results_dataset: Optional[gdal.Band]
) -> float:

    # all layers now have the same window width/height, so just take the habitat one
    pixel_width = habitat_layer.window.xsize
    pixel_height = habitat_layer.window.ysize

    aoh_shader = cupy.ElementwiseKernel(
        'bool habitat, int16 elevation, uint8 species_range, float64 pixel_area',
        'float64 result',
        'result = (species_range && habitat && ' \
            f'((elevation >= {min(elevation_range)}) && (elevation <= {max(elevation_range)})));' \
            'result = result * pixel_area',
        'my_shader'
    )
    aoh_reduction_shader = cupy.ReductionKernel(
        'bool habitat, int16 elevation, uint8 species_range, float64 pixel_area',
        'float64 result',
        f'(species_range && habitat && ((elevation >= {min(elevation_range)}) && ' \
            f'(elevation <= {max(elevation_range)}))) * pixel_area',
        'a + b',
        'result = a',
        '0.0',
        'my_reduction_shader'
    )

    habitat_list = cupy.array(habitat_list)

    area_total = 0.0
    data = None
    for yoffset in range(0, pixel_height, YSTEP):
        this_step = YSTEP
        if yoffset + this_step > pixel_height:
            this_step = pixel_height - yoffset

        habitat, elevation, species_range, pixel_areas = [
            cupy.array(x.read_array(0, yoffset, pixel_width, this_step))
            for x in [habitat_layer, elevation_layer, range_layer, area_layer]
        ]

        filtered_habitat = cupy.isin(habitat, habitat_list)

        # if we don't need to store out the geotiff then we can do
        # the summation and sum in a single reduction shader. Otherwise we need to
        # calc to an area and then reduce, which is slower but is the price of
        # getting the intermediary data
        if not results_dataset:
            area_total += aoh_reduction_shader(filtered_habitat, elevation, species_range, pixel_areas)
        else:
            if not data:
                data = cupy.same_as(filtered_habitat)
            aoh_shader(filtered_habitat, elevation, species_range, pixel_areas, data)
            area_total += cupy.sum(data)
            results_dataset.WriteArray(data.get(), 0, yoffset)

    return area_total
