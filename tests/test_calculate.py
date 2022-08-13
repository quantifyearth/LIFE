from typing import Any

import numpy
import pytest
from osgeo import gdal

from persistence.layers import Layer, Window, UniformAreaLayer
import persistence

class SingleValueLayer(Layer):
	"""Mocked layer to make testing calc function easier"""
	def __init__(self, value: Any, width: int, height: int):
		self.value = value
		self.window = Window(0, 0, width, height)

	def read_array(self, xoffset: int, yoffset: int, xsize: int, ysize: int) -> Any:
		assert (xoffset + xsize) <= self.window.xsize
		assert (yoffset + ysize) <= self.window.ysize
		return numpy.ones((ysize, xsize)) * self.value

@pytest.mark.parametrize(
	"habitat,elevation,range,area,habitats,elevation_range,expected_area",
	[
		(100, 1234.0, True, 4.0, [100, 200, 300], (0.0, 10000.0), 4.0),
		(100, 1234.0, False, 4.0, [100, 200, 300], (0.0, 10000.0), 0.0),
		(100, 1234.0, True, 4.0, [200, 300], (0.0, 10000.0), 0.0),
		(100, 1234.0, True, 4.0, [100, 200, 300], (0.0, 100.0), 0.0),
		(100, 1234.0, True, numpy.nan, [100, 200, 300], (0.0, 10000.0), 0.0),
	]
)
def test_calculate_simple(habitat,elevation,range,area,habitats,elevation_range,expected_area):
	habitat_layer = SingleValueLayer(habitat, 1, 1)
	elevation_layer = SingleValueLayer(elevation, 1, 1)
	range_layer = SingleValueLayer(range, 1, 1)
	area_layer = SingleValueLayer(area, 1, 1)

	persistence.YSTEP = 1
	area = persistence._calculate_cpu(
		range_layer,
		habitat_layer,
		habitats,
		elevation_layer,
		elevation_range,
		area_layer,
		None
	)
	assert area == expected_area

@pytest.mark.parametrize("step_size", [1, 2, 3, 9, 10, 11])
def test_calculate_step_sizes(step_size):
	habitat_layer = SingleValueLayer(100, 10, 10)
	elevation_layer = SingleValueLayer(1234.0, 10, 10)
	range_layer = SingleValueLayer(True, 10, 10)

	# we want a non uniform area to make this interesting
	area_dataset = gdal.GetDriverByName('mem').Create('mem', 1, 10, 1, gdal.GDT_Float32, [])
	area_dataset.SetGeoTransform([-180.0, 180.0, 0.0, 90.0, 0.0, -18.0])
	area_dataset.GetRasterBand(1).WriteArray(numpy.array([[float(x)] for x in range(1, 11)]), 0, 0)
	area_layer = UniformAreaLayer(area_dataset)

	persistence.YSTEP = step_size
	area = persistence._calculate_cpu(
		range_layer,
		habitat_layer,
		[100, 200, 300],
		elevation_layer,
		(0.0, 10000.0),
		area_layer,
		None
	)
	assert area == 550.0
