
from layers import Layer, Area
from helpers import make_dataset_of_region

def test_make_basic_layer() -> None:
	area = Area(-10, 10, 10, -10)
	layer = Layer(make_dataset_of_region(area, 0.02))
	assert layer.area == area
