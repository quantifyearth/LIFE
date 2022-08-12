from typing import Any

import numpy
import pytest

from persistence.layers import Layer, Window
from persistence import _calculate_cpu

class SingleValueLayer(Layer):
	"""Mocked layer to make testing calc function easier"""
	def __init__(self, value: Any):
		self.value = value
		self.window = Window(0, 0, 1, 1)

	def read_array(self, _xoffset: int, _yoffset: int, _xsize: int, _ysize: int) -> Any:
		return numpy.array([[self.value]])

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
	habitat_layer = SingleValueLayer(habitat)
	elevation_layer = SingleValueLayer(elevation)
	range_layer = SingleValueLayer(range)
	area_layer = SingleValueLayer(area)
	area = _calculate_cpu(
		range_layer,
		habitat_layer,
		habitats,
		elevation_layer,
		elevation_range,
		area_layer,
		None
	)
	assert area == expected_area
