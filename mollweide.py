import argparse
import os
import shutil
import sys
import tempfile

import numpy as np
import pandas as pd
import h3
from osgeo import gdal
from pyproj import CRS, Transformer

TARGET_WIDTH=7267
TARGET_HEIGHT=3385

EXTENT_MIN_X, EXTENT_MAX_X, EXTENT_MIN_Y, EXTENT_MAX_Y = -18027854.1353249, 18027101.8531421, -7965787.75894896, 8828766.53043604

PIXEL_SCALE_X = (EXTENT_MAX_X - EXTENT_MIN_X) / TARGET_WIDTH
PIXEL_SCALE_Y = (EXTENT_MAX_Y - EXTENT_MIN_Y) / TARGET_HEIGHT

PROJ = """PROJCRS[\"Mollweide\",\n    BASEGEOGCRS[\"WGS 84\",\n        DATUM[\"D_unknown\",\n            ELLIPSOID[\"WGS84\",6378137,298.257223563,\n                LENGTHUNIT[\"metre\",1,\n                    ID[\"EPSG\",9001]]]],\n        PRIMEM[\"Greenwich\",0,\n            ANGLEUNIT[\"Degree\",0.0174532925199433]]],\n    CONVERSION[\"unnamed\",\n        METHOD[\"Mollweide\"],\n        PARAMETER[\"Longitude of natural origin\",0,\n            ANGLEUNIT[\"Degree\",0.0174532925199433],\n            ID[\"EPSG\",8802]],\n        PARAMETER[\"False easting\",0,\n            LENGTHUNIT[\"metre\",1],\n            ID[\"EPSG\",8806]],\n        PARAMETER[\"False northing\",0,\n            LENGTHUNIT[\"metre\",1],\n            ID[\"EPSG\",8807]]],\n    CS[Cartesian,2],\n        AXIS[\"(E)\",east,\n            ORDER[1],\n            LENGTHUNIT[\"metre\",1,\n                ID[\"EPSG\",9001]]],\n        AXIS[\"(N)\",north,\n            ORDER[2],\n            LENGTHUNIT[\"metre\",1,\n                ID[\"EPSG\",9001]]]]"""

def generate_mollweide(
	tiles_csv_filename: str,
	output_filename: str,
) -> None:
	df = pd.read_csv(tiles_csv_filename)

	wgs85_crs = CRS.from_string("EPSG:4326")
	mollweide_crs = CRS.from_string(PROJ)
	transformer = Transformer.from_crs(wgs85_crs, mollweide_crs, always_xy=True)

	# work out the pixel scale
	# x_scale = (transformer.transform(180, 0)[0] * 2.0) / TARGET_WIDTH
	# y_scale = (transformer.transform(0, 90)[1] * 2.0) / TARGET_HEIGHT
	x_scale = PIXEL_SCALE_X
	y_scale = PIXEL_SCALE_Y
	print(f"pixel scale: {x_scale}, {y_scale}")

	raw = np.zeros((TARGET_HEIGHT, TARGET_WIDTH)).tolist()

	with tempfile.TemporaryDirectory() as tempdir:
		tempname = os.path.join(tempdir, "result.tif")
		output_dataset = gdal.GetDriverByName("gtiff").Create(
			tempname,
			TARGET_WIDTH,
			TARGET_HEIGHT,
			1,
			gdal.GDT_Float64,
			['COMPRESS=LZW'],
		)
		output_dataset.SetProjection(PROJ)
		output_dataset.SetGeoTransform((
				EXTENT_MIN_X, x_scale, 0.0,
				EXTENT_MIN_Y, 0.0, y_scale
		))
		band = output_dataset.GetRasterBand(1)

		for _, row in df.iterrows():
			tileid, area = row
			try:
				lat, lng = h3.cell_to_latlng(tileid)
			except ValueError:
				print(f"Failed to process {tileid}")
				continue
			x_mollweide, y_mollweide = transformer.transform(lng, lat)
			x_mollweide -= EXTENT_MAX_X
			y_mollweide -= EXTENT_MIN_Y

			xpos = round((x_mollweide / x_scale))
			ypos = round((y_mollweide / y_scale))
			val = raw[ypos][xpos]
			if val == 0:
				val = [area]
			else:
				val.append(area)
			raw[ypos][xpos] = val

		# Now we need to average all the cells
		for yoffset in range(TARGET_HEIGHT):
			for xoffset in range(TARGET_WIDTH):
				val = raw[yoffset][xoffset]
				raw[yoffset][xoffset] = np.mean(val)

		band.WriteArray(np.array(raw), 0, 0)
		del output_dataset

		shutil.move(tempname, output_filename)

def main() -> None:
	parser = argparse.ArgumentParser()
	parser.add_argument(
		"--tiles",
		type=str,
		required=True,
		dest="tiles_csv_filename",
		help="CSV containing h3 tiles and values."
	)
	parser.add_argument(
		"--output",
		type=str,
		required=True,
		dest="output_filename",
		help="Filename for output GeoTIFF."
	)
	args = parser.parse_args()

	generate_mollweide(args.tiles_csv_filename, args.output_filename)

if __name__ == "__main__":
	main()
