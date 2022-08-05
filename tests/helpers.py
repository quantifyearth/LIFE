from math import ceil

import numpy
from osgeo import gdal

from layers import Layer, Area

def make_dataset_of_region(area: Area, pixel_pitch: float, filename=None) -> gdal.Dataset:
	if filename:
		driver = gdal.GetDriverByName('GTiff')
	else:
		driver = gdal.GetDriverByName('mem')
		filename = 'mem'
	dataset = driver.Create(
		filename,
		ceil((area.right - area.left) / pixel_pitch),
		ceil((area.top - area.bottom) / pixel_pitch),
		1,
		gdal.GDT_Byte,
		[]
	)
	dataset.SetGeoTransform([
		area.left, pixel_pitch, 0.0, area.top, 0.0, pixel_pitch * -1.0
	])
	dataset.SetProjection("WGS 84")
	# the dataset isn't valid until you populate the data
	band = dataset.GetRasterBand(1)
	for yoffset in range(dataset.RasterYSize):
		band.WriteArray(numpy.array([[(yoffset % 256),] * dataset.RasterXSize]), 0, yoffset)
	return dataset
