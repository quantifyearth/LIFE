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

def generate_mollweide(
	tiles_csv_filename: str,
	output_filename: str,
) -> None:
	df = pd.read_csv(tiles_csv_filename)

	wgs85_crs = CRS.from_string("EPSG:4326")
	mollweide_crs = CRS.from_string("ESRI:54009")
	transformer = Transformer.from_crs(wgs85_crs, mollweide_crs, always_xy=True)

	# work out the pixel scale
	x_scale = (transformer.transform(180, 0)[0] * 2.0) / TARGET_WIDTH
	y_scale = (transformer.transform(0, 90)[1] * 2.0) / TARGET_HEIGHT
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
		band = output_dataset.GetRasterBand(1)

		for _, row in df.iterrows():
			tileid, area = row
			try:
				lat, lng = h3.cell_to_latlng(tileid)
			except ValueError:
				print(f"Failed to process {tileid}")
				continue
			x_mollweide, y_mollweide = transformer.transform(lng, lat)
			xpos = round((x_mollweide / x_scale) + (TARGET_WIDTH / 2))
			ypos = round((TARGET_HEIGHT / 2) - (y_mollweide / y_scale))
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
