import sys

from osgeo import gdal

from yirgacheffe import WSG_84_PROJECTION
from yirgacheffe.layers import Layer, PixelScale, Area
from yirgacheffe.h3layer import H3CellLayer

if len(sys.argv) != 3:
	print(f'USAGE: {sys.argv[0]} CSV TIF')
	sys.exit(-1)

# Make up the geo transform based on image resolution
width, height = 3840.0, 2180.0 # 4K screen
scale = PixelScale(360.0 / width, -180.0/height)
area = Area(left=-180.0, right=180, top=90, bottom=-90)

# I should use pandas for this, but that needs me to have more than five minutes to
# learn it, sorry
with open(sys.argv[1]) as f:
	rawdata = f.readlines()[1:]
data = []
for line in rawdata:
	parts = line.split(',')
	data.append((parts[0], float(parts[1].strip())))

scratch = Layer.empty_raster_layer(area, scale, gdal.GDT_Float64)

# work in progress...
band = scratch._dataset.GetRasterBand(1)

for index in range(len(data)):
	tile, area = data[index]
	if area == 0.0:
		continue
	tileLayer = H3CellLayer(tile, scale, WSG_84_PROJECTION)

	layers = [scratch, tileLayer]
	intersection = Layer.find_intersection(layers)
	for layer in layers:
		layer.set_window_for_intersection(intersection)

	result = scratch + (tileLayer * area)
	result.save(band)

# now we've done the calc in memory, save it to a file
output = Layer.empty_raster_layer_like(scratch, filename=sys.argv[2])

scratch.reset_window()
scratch.save(output._dataset.GetRasterBand(1))
