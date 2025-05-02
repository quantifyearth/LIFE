import argparse
import os
import sys
import time
from multiprocessing import Manager, Process, Queue, cpu_count
from pathlib import Path

import geopandas as gpd
from shapely.lib import intersects
import pandas as pd
import shapely
from pyogrio.errors import DataSourceError

def process_species_worker(
	project_polygon_path: str,
	input_queue: Queue,
	result_queue: Queue,
) -> None:
	project_df = gpd.read_file(project_polygon_path)
	polygons = project_df.loc[project_df.geometry.geometry.type.isin(['Polygon', 'MultiPolygon'])]
	project_polygon = shapely.union_all([x.geometry for (_, x) in polygons.iterrows()])

	while True:
		paths = input_queue.get()
		if paths is None:
			break

		try:
			species_infos = []
			for path in paths:
				with open(path) as f:
					species_infos.append(shapely.from_geojson(f.read()))
		except (DataSourceError, shapely.errors.GEOSException):
			print("load error: ", paths)
			continue

		if len(species_infos) == 1:
			geometry = species_infos[0]
		else:
			try:
				geometry = shapely.union_all(species_infos)
			except shapely.errors.GEOSException:
				print("union error: ", paths)
				print("union error: ", species_infos)
				continue

		state = "external"
		if project_polygon.contains(geometry):
			state = "endemic"
		elif project_polygon.intersects(geometry):
			state = "non-endemic"

		result_queue.put((paths[0], state))

	result_queue.put(None)


def overlap(
	project_polygon_path: str,
	species_infos_path: str,
	output_filename: str,
	processes_count: int,
) -> None:
	result_dir, filename = os.path.split(output_filename)
	os.makedirs(result_dir, exist_ok=True)

	raw_species_infos = list(Path(species_infos_path).glob("**/historic/*.geojson"))
	print(f"We collected {len(raw_species_infos)} files")

	species_map = {}
	for species_info in raw_species_infos:
		_, filename = os.path.split(species_info)
		taxid = int(filename.split('_')[0])
		try:
			species_map[taxid].append(species_info)
		except KeyError:
			species_map[taxid] = [species_info]

	print(f"We collected {len(species_map)} species")

	with Manager() as manager:
		source_queue = manager.Queue()
		result_queue = manager.Queue()

		workers = [Process(target=process_species_worker, args=(
			project_polygon_path,
			source_queue,
			result_queue,
		)) for index in range(processes_count)]
		for worker_process in workers:
			worker_process.start()

		for files in species_map.values():
			source_queue.put(files)
		for _ in range(len(workers)):
			source_queue.put(None)

		processes = workers
		while processes:
			candidates = [x for x in processes if not x.is_alive()]
			for candidate in candidates:
				candidate.join()
				if candidate.exitcode:
					for victim in processes:
						victim.kill()
					sys.exit(candidate.exitcode)
				processes.remove(candidate)
			time.sleep(1)

		results = set()
		sentinel_counter = 0
		while True:
			val = result_queue.get()
			if val is None:
				sentinel_counter += 1
				if sentinel_counter == processes_count:
					break
			else:
				path, state = val
				directory, filename = os.path.split(path)
				taxa = directory.split("/")[-2]
				taxonid = int(filename.split('_')[0])
				results.add((taxonid, taxa, state))

		df = pd.DataFrame(results, columns = ["id_no", "class", "state"])
		df.to_csv(output_filename, index=False)

def main() -> None:
	parser = argparse.ArgumentParser(description="Find species that overlap with area.")
	parser.add_argument(
		"--polygon",
		type=str,
		required=True,
		dest="project_polygon_path",
		help="Border of area for included species."
	)
	parser.add_argument(
		"--species_info",
		type=str,
		required=True,
		dest="species_infos_path",
		help="Path of folder with species_info in from LIFE run"
	)
	parser.add_argument(
		"--output",
		type=str,
		required=True,
		dest="output_path",
		help="CSV into which to write species data."
	)
	parser.add_argument(
		"-j",
		type=int,
		required=False,
		default=round(cpu_count() / 2),
		dest="processes_count",
		help="Number of concurrent threads to use."
	)
	args = parser.parse_args()

	overlap(
		args.project_polygon_path,
		args.species_infos_path,
		args.output_path,
		args.processes_count,
	)

if __name__ == "__main__":
	main()
