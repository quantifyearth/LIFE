import pytest

from helpers import make_dataset_of_region
from persistence.layers import Area, Layer, NullLayer, Window


def test_find_union_empty_list() -> None:
	with pytest.raises(ValueError):
		Layer.find_union([])

def test_find_union_single_item() -> None:
	layer = Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02))
	union = Layer.find_union([layer])
	assert union == layer.area

def test_find_union_same() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02))
	]
	union = Layer.find_union(layers)
	assert union == layers[0].area

def test_find_union_subset() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		Layer(make_dataset_of_region(Area(-1, 1, 1, -1), 0.02))
	]
	union = Layer.find_union(layers)
	assert union == layers[0].area

def test_find_union_overlap() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		Layer(make_dataset_of_region(Area(-15, 15, -5, -5), 0.02))
	]
	union = Layer.find_union(layers)
	assert union == Area(-15, 15, 10, -10)

def test_find_union_distinct() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-110, 10, -100, -10), 0.02)),
		Layer(make_dataset_of_region(Area(100, 10, 110, -10), 0.02))
	]
	union = Layer.find_union(layers)
	assert union == Area(-110, 10, 110, -10)

def test_find_union_with_null() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		NullLayer()
	]
	union = Layer.find_union(layers)
	assert union == layers[1].area

def test_find_union_different_pixel_pitch() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		Layer(make_dataset_of_region(Area(-15, 15, -5, -5), 0.01))
	]
	with pytest.raises(ValueError):
		_ = Layer.find_union(layers)

def test_set_union_superset() -> None:
	layer = Layer(make_dataset_of_region(Area(-1, 1, 1, -1), 0.02))
	assert layer.window == Window(0, 0, 100, 100)
	origin_before_pixel = layer.read_array(0, 0, 100, 1)

	# Superset only extends on both sides
	superset = Area(-2.0, 1.0, 2.0, -1.0)
	layer.set_window_for_union(superset)
	assert layer.window == Window(-50, 0, 200, 100)

	# Origin should be zero value
	origin_after_pixel = layer.read_array(0, 0, 200, 1)
	assert origin_after_pixel is not None
	assert origin_after_pixel[0][0] == 0.0

	# But we should be able to find the original pixels in there too
	assert origin_before_pixel[0][0] == origin_after_pixel[0][50]


