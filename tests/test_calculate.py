from typing import Any

import numpy
import pytest

from layers import Layer, Window
from persistence import _calculate

class SingleValueLayer(Layer):
	def __init__(self, value: Any):
		self.value = value
		self.window = Window(0, 0, 1, 1)

	def read_array(self, _xoffset, _yoffset, _xsize, _ysize) -> Any:
		return numpy.array([[self.value]])

@pytest.mark.parametrize(
	"habitat,elevation,range,area,habitats,elevation_range,expected_area",
	[
		(100, 1234.0, True, 4.0, [100, 200, 300], (0.0, 10000.0), 4.0),
		(100, 1234.0, False, 4.0, [100, 200, 300], (0.0, 10000.0), 0.0),
		(100, 1234.0, True, 4.0, [200, 300], (0.0, 10000.0), 0.0),
		(100, 1234.0, True, 4.0, [100, 200, 300], (0.0, 100.0), 0.0),
	]
)
def test_calculate_simple(habitat,elevation,range,area,habitats,elevation_range,expected_area):
	habitat_layer = SingleValueLayer(habitat)
	elevation_layer = SingleValueLayer(elevation)
	range_layer = SingleValueLayer(range)
	area_layer = SingleValueLayer(area)
	area = _calculate(
		range_layer,
		habitat_layer,
		habitats,
		elevation_layer,
		elevation_range,
		area_layer,
		None
	)
	assert area == expected_area
