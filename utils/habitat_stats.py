import argparse
import os
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd
from yirgacheffe.layers import RasterLayer, UniformAreaLayer

from osgeo import gdal
gdal.SetCacheMax(1024 * 1024 * 16)

def habitat_stats(
	habitats_dir: str,
	area_map_path: str,
	output_dir: str,
	process_count: int,
) -> None:
	with UniformAreaLayer.layer_from_file(area_map_path) as area_raster:
		os.makedirs(output_dir, exist_ok=True)
		for scenario in Path(habitats_dir).iterdir():
			res = []
			for habitat in scenario.glob("*.tif"):
				# Filenames have the form "lcc_200.tif" - we want the IUCN habitat class 2 or 14.1 etc."
				basename, _ = os.path.splitext(habitat.name)
				jung_habitat_class = int(basename.split("_")[1])
				if jung_habitat_class == 0:
					continue
				with RasterLayer.layer_from_file(str(habitat)) as raster:
					raw_area = raster.parallel_sum(parallelism=process_count)
					scaled_area_calc = raster * area_raster
					scaled_area = scaled_area_calc.parallel_sum(parallelism=process_count)

				res.append([jung_habitat_class, raw_area, scaled_area])
			df = pd.DataFrame(res, columns=["habitat", "pixel area", "geo area"])
			df = df.sort_values("habitat")
			df.to_csv(os.path.join(output_dir, f"{scenario.name}.csv"), index=False)

def main() -> None:
	parser = argparse.ArgumentParser(description="Generate stats on the habitat makeup of each scenario")
	parser.add_argument(
		"--habitats",
		type=str,
		required=True,
		dest="habitats_dir",
	)
	parser.add_argument(
		"--area",
		type=str,
		required=True,
		dest="area_map_path",
	)
	parser.add_argument(
		"--output",
		type=str,
		required=True,
		dest="output_dir",
		help="Destination directory for CSVs"
	)
	parser.add_argument(
		"-j",
		type=int,
		required=False,
		default=round(cpu_count() / 2),
		dest="process_count",
		help="Number of parallel threads to use."
	)
	args = parser.parse_args()

	habitat_stats(
		args.habitats_dir,
		args.area_map_path,
		args.output_dir,
		args.process_count,
	)

if __name__ == "__main__":
	main()
