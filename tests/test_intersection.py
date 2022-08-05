import pytest

from helpers import make_dataset_of_region
from layers import Area, Layer, NullLayer


def test_find_intersection_empty_list() -> None:
	with pytest.raises(ValueError):
		Layer.find_intersection([])

def test_find_intersection_single_item() -> None:
	layer = Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02))
	intersection = Layer.find_intersection([layer])
	assert intersection == layer.area

def test_find_intersection_same() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02))
	]
	intersection = Layer.find_intersection(layers)
	assert intersection == layers[0].area

def test_find_intersection_subset() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		Layer(make_dataset_of_region(Area(-1, 1, 1, -1), 0.02))
	]
	intersection = Layer.find_intersection(layers)
	assert intersection == layers[1].area

def test_find_intersection_overlap() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		Layer(make_dataset_of_region(Area(-15, 15, -5, -5), 0.02))
	]
	intersection = Layer.find_intersection(layers)
	assert intersection == Area(-10, 10, -5, -5)

def test_find_intersection_distinct() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-110, 10, -100, -10), 0.02)),
		Layer(make_dataset_of_region(Area(100, 10, 110, -10), 0.02))
	]
	with pytest.raises(ValueError):
		_ = Layer.find_intersection(layers)

def test_find_intersection_with_null() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		NullLayer()
	]
	intersection = Layer.find_intersection(layers)
	assert intersection == layers[0].area

def test_find_intersection_different_pixel_pitch() -> None:
	layers = [
		Layer(make_dataset_of_region(Area(-10, 10, 10, -10), 0.02)),
		Layer(make_dataset_of_region(Area(-15, 15, -5, -5), 0.01))
	]
	with pytest.raises(ValueError):
		_ = Layer.find_intersection(layers)
