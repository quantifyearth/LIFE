from math import ceil

from osgeo import gdal

from layers import Layer, Area

def make_dataset_of_region(area: Area, pixel_pitch: float) -> gdal.Dataset:
	dataset = gdal.GetDriverByName('mem').Create(
		'mem',
		ceil((area.right - area.left) / pixel_pitch),
		ceil((area.top - area.bottom) / pixel_pitch),
		1,
		gdal.GDT_Byte,
		[]
	)
	dataset.SetGeoTransform([
		area.left, pixel_pitch, 0.0, area.top, 0.0, pixel_pitch * -1.0
	])
	return dataset
