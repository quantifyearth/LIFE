import argparse
import glob
import os
import re
import sys
import tempfile
import time
from multiprocessing import Manager, Process, Queue, cpu_count
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from yirgacheffe.layers import RasterLayer, YirgacheffeLayer  # type: ignore
from osgeo import gdal

def worker(
    filename: str,
    result_dir: str,
    input_queue: Queue,
) -> None:
    output_tif = os.path.join(result_dir, filename)

    merged_result = None

    while True:
        path = input_queue.get()
        if path is None:
            break

        with RasterLayer.layer_from_file(path) as partial_raster:
            if merged_result is None:
                merged_result = RasterLayer.empty_raster_layer_like(partial_raster, datatype=gdal.GDT_Float64)
                cleaned_raster = partial_raster.numpy_apply(lambda chunk: np.nan_to_num(chunk, copy=False, nan=0.0))
                cleaned_raster.save(merged_result)
            else:
                merged_result.reset_window()

                union = YirgacheffeLayer.find_union([merged_result, partial_raster])
                merged_result.set_window_for_union(union)
                partial_raster.set_window_for_union(union)

                calc = merged_result + (
                    partial_raster.numpy_apply(lambda chunk: np.nan_to_num(chunk, copy=False, nan=0.0))
                )
                temp = RasterLayer.empty_raster_layer_like(merged_result, datatype=gdal.GDT_Float64)
                calc.save(temp)
                merged_result = temp

    if merged_result:
        final = RasterLayer.empty_raster_layer_like(merged_result, filename=output_tif)
        merged_result.save(final)

def process(
    filters: List[Tuple[str,str]],
    collated_metadata: pd.DataFrame,
    raster_files: Dict[int, str],
    processes_count: int,
    result_dir: str,
) -> None:
    subset = collated_metadata
    for key, value in filters:
        subset = subset[subset[key] == value]

    rasters = []
    for _, row in subset.iterrows():
        try:
            raster = raster_files[row.id_no]
            rasters.append(raster)
        except KeyError:
            continue

    filename = "_".join([x[1] for x in filters]) + ".tif"
    print(filename, len(subset), len(rasters))

    with tempfile.TemporaryDirectory() as tempdir:
        with Manager() as manager:
            source_queue = manager.Queue()

            workers = [Process(target=worker, args=(
                f"{index}.tif",
                tempdir,
                source_queue
            )) for index in range(processes_count)]
            for worker_process in workers:
                worker_process.start()

            for file in rasters:
                source_queue.put(file)
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

            # here we should have now a set of images in tempdir to merge
            single_worker = Process(target=worker, args=(
                filename,
                result_dir,
                source_queue
            ))
            single_worker.start()
            nextfiles = [os.path.join(tempdir, x) for x in glob.glob("*.tif", root_dir=tempdir)]
            for file in nextfiles:
                source_queue.put(file)
            source_queue.put(None)

            processes = [single_worker]
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

def recursive_process(
    slice_values: List[Tuple[str, List[str]]],
    filters: List[Tuple[str,str]],
    collated_metadata: pd.DataFrame,
    raster_files: Dict[int, str],
    processes_count: int,
    result_dir: str,
) -> None:
    if not slice_values:
        process(filters, collated_metadata, raster_files, processes_count, result_dir)
    else:
        var, varlist = slice_values[0]
        tail = slice_values[1:]

        for value in varlist:
            recursive_process(tail, filters + [(var, value)], collated_metadata,
				raster_files, processes_count, result_dir)

def deltap_sum(
    images_dir: str,
    collated_metadata_path: str,
    slices: List[str],
    output_directory: str,
    processes_count: int
) -> None:
    try:
        collated_metadata = pd.read_csv(collated_metadata_path)
    except FileNotFoundError:
        sys.exit(f"Failed to find {collated_metadata_path}")

    os.makedirs(output_directory, exist_ok=True)

    files = [os.path.join(images_dir, x) for x in glob.glob("**/*.tif", root_dir=images_dir, recursive=True)]
    if not files:
        sys.exit(f"No files in {images_dir}, aborting")
    idre = re.compile(r".*/(\d+)_.*")
    raster_file_map = {}
    for f in files:
        try:
            id_no = int(idre.match(f).groups()[0]) # type: ignore[union-attr]
            raster_file_map[id_no] = os.path.join(images_dir, f)
        except AttributeError:
            print(f)


    slice_values : List = [
        (k, collated_metadata[k].unique()) for k in slices
    ]
    recursive_process(slice_values, [], collated_metadata, raster_file_map, processes_count, output_directory)

def main() -> None:
    parser = argparse.ArgumentParser(description="Generates a single raster of M from all the per K rasters.")
    parser.add_argument(
        "--rasters_directory",
        type=str,
        required=True,
        dest="rasters_directory",
        help="Folder containing all the deltap rasters for a given scenario in their respective subdirectories."
    )
    parser.add_argument(
        "--collated_metadata",
        type=str,
        required=True,
        dest="collated_metadata",
        help="A single CSV file with the collated metadata from the AoHs."
    )
    parser.add_argument(
        "--slices",
        default=["class_name"],
        choices=['class_name', 'category'],
        nargs="*",
        dest="slices",
        help="List of axis on which to split summing."
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        dest="output_directory",
        help="Destination directory for geotiff outputs."
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

    deltap_sum(
        args.rasters_directory,
        args.collated_metadata,
        args.slices,
        args.output_directory,
        args.processes_count
    )

if __name__ == "__main__":
    main()
