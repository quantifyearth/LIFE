import argparse
import glob
import os
import sys
import tempfile
import time
from multiprocessing import Manager, Process, Queue, cpu_count

import numpy as np
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

                calc = merged_result + (partial_raster.numpy_apply(lambda chunk: np.nan_to_num(chunk, copy=False, nan=0.0)))
                temp = RasterLayer.empty_raster_layer_like(merged_result, datatype=gdal.GDT_Float64)
                calc.save(temp)
                merged_result = temp

    if merged_result:
        final = RasterLayer.empty_raster_layer_like(merged_result, filename=output_tif)
        merged_result.save(final)

def build_k(
    images_dir: str,
    output_filename: str,
    processes_count: int
) -> None:
    result_dir, filename = os.path.split(output_filename)
    os.makedirs(result_dir, exist_ok=True)

    files = [os.path.join(images_dir, x) for x in glob.glob("*.tif", root_dir=images_dir)]
    if not files:
        print(f"No files in {images_dir}, aborting", file=sys.stderr)
        sys.exit(-1)

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

            for file in files:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Generates a single raster of M from all the per K rasters.")
    parser.add_argument(
        "--rasters_directory",
        type=str,
        required=True,
        dest="rasters_directory",
        help="GeoTIFF file containing pixels in set M as generated by calculate_k.py"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        dest="output_filename",
        help="Destination parquet file for results."
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

    build_k(
        args.rasters_directory,
        args.output_filename,
        args.processes_count
    )

if __name__ == "__main__":
    main()
