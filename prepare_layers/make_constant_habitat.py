import argparse
import math
import os
from pathlib import Path

import pandas as pd
from yirgacheffe.layers import ConstantLayer, RasterLayer # type: ignore

def make_constant_habitat(
	example_path: Path,
	habitat_code: str,
	crosswalk_path: Path,
	output_path: Path,
) -> None:
	os.makedirs(output_path, exist_ok=True)
	crosswalk = pd.read_csv(crosswalk_path)
	translations = crosswalk[crosswalk.code==habitat_code]
	specific_jung_code = list(translations.value)[-1]
	filename = output_path / f"lcc_{specific_jung_code}.tif"
	with RasterLayer.layer_from_file(example_path) as example:
		with RasterLayer.empty_raster_layer_like(example, filename=filename) as result:
			ConstantLayer(1.0).save(result)

def main() -> None:
	parser = argparse.ArgumentParser(description="Generate a fixed habitat layer")
	parser.add_argument(
		'--examplar',
		type=Path,
		help='Example processed habitat layer',
		required=True,
		dest='example_path',
	)
	parser.add_argument(
		'--habitat_code',
		type=str,
		help='IUCN habitat code',
		required=True,
		dest='habitat_code',
	)
	parser.add_argument(
		'--crosswalk',
		type=Path,
		help='Path of map to IUCN crosswalk table',
		required=True,
		dest='crosswalk_path',
	)
	parser.add_argument(
		'--output',
		type=Path,
		help='Directory where final map should be stored',
		required=True,
		dest='results_path',
	)
	args = parser.parse_args()

	make_constant_habitat(
		args.example_path,
		args.habitat_code,
		args.crosswalk_path,
		args.results_path,
	)

if __name__ == "__main__":
	main()
