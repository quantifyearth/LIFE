import os
import tempfile

import pytest

from layers import Area, Layer, Window
from helpers import make_dataset_of_region

def test_make_basic_layer() -> None:
	area = Area(-10, 10, 10, -10)
	layer = Layer(make_dataset_of_region(area, 0.02))
	assert layer.area == area
	assert layer.pixel_scale == (0.02, -0.02)
	assert layer.geo_transform == (-10, 0.02, 0.0, 10, 0.0, -0.02)
	assert layer.window == Window(0, 0, 1000, 1000)

def test_layer_from_null() -> None:
	# Seems a petty test, but gdal doesn't throw exceptions
	# so you often get None datasets if you're not careful
	with pytest.raises(ValueError):
		Layer(None)

def test_layer_from_nonexistent_file() -> None:
	with pytest.raises(FileNotFoundError):
		Layer.layer_from_file("this_file_does_not_exist.tif")

def test_open_file() -> None:
	with tempfile.TemporaryDirectory() as tempdir:
		path = os.path.join(tempdir, "test.tif")
		area = Area(-10, 10, 10, -10)
		_ = make_dataset_of_region(area, 0.02, filename=path)
		assert os.path.exists(path)
		layer = Layer.layer_from_file(path)
		assert layer.area == area
		assert layer.pixel_scale == (0.02, -0.02)
		assert layer.geo_transform == (-10, 0.02, 0.0, 10, 0.0, -0.02)
		assert layer.window == Window(0, 0, 1000, 1000)
