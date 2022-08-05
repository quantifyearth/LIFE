
from layers import Area, Layer, Window
from helpers import make_dataset_of_region

def test_make_basic_layer() -> None:
	area = Area(-10, 10, 10, -10)
	layer = Layer(make_dataset_of_region(area, 0.02))
	assert layer.area == area
	assert layer.pixel_scale == (0.02, -0.02)
	assert layer.geo_transform == (-10, 0.02, 0.0, 10, 0.0, -0.02)
	assert layer.window == Window(0, 0, 1000, 1000)
