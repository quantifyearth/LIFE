import argparse
import os
import queue
import sys
import tempfile
import time
from pathlib import Path
from multiprocessing import Manager, Process, Queue, cpu_count
from typing import Optional

from yirgacheffe.layers import RasterLayer  # type: ignore
from yirgacheffe.operators import DataType

def worker(
    compress: bool,
    filename: str,
    result_dir: Path,
    input_queue: Queue,
) -> None:
    output_tif = result_dir / filename

    merged_result: Optional[RasterLayer] = None

    while True:
        try:
            path: Path = input_queue.get_nowait()
        except queue.Empty:
            break
        if compress:
            print(path)

        with RasterLayer.layer_from_file(path) as partial_raster:
            if merged_result is None:
                merged_result = RasterLayer.empty_raster_layer_like(partial_raster, datatype=DataType.Float64)
                cleaned_raster = partial_raster.nan_to_num(nan=0.0)
                cleaned_raster.save(merged_result)
            else:
                calc = merged_result + partial_raster.nan_to_num(nan=0.0)
                temp = RasterLayer.empty_raster_layer_like(calc, datatype=DataType.Float64)
                calc.save(temp)
                merged_result = temp

    if merged_result:
        with RasterLayer.empty_raster_layer_like(merged_result, filename=output_tif, compress=compress) as final:
            merged_result.save(final)
        input_queue.put(output_tif)

def raster_sum(
    images_dir: Path,
    output_filename: Path,
    processes_count: int
) -> None:
    os.makedirs(output_filename.parent, exist_ok=True)

    files = images_dir.glob("*.tif")
    if not files:
        sys.exit(f"No files in {images_dir}, aborting")

    with tempfile.TemporaryDirectory() as tempdir:
        with Manager() as manager:
            source_queue = manager.Queue()

            workers = [Process(target=worker, args=(
                False,
                f"{index}.tif",
                Path(tempdir),
                source_queue
            )) for index in range(processes_count)]
            for worker_process in workers:
                worker_process.start()

            for file in files:
                source_queue.put(file)
            # for _ in range(len(workers)):
            #     source_queue.put(None)

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
                time.sleep(0.1)


            # here we should have now a set of images in tempdir to merge
            single_worker = Process(target=worker, args=(
                True,
                output_filename.name,
                output_filename.parent,
                source_queue
            ))
            single_worker.start()

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
    parser = argparse.ArgumentParser(description="Sums many rasters into a single raster")
    parser.add_argument(
        "--rasters_directory",
        type=Path,
        required=True,
        dest="rasters_directory",
        help="Folder containing all the deltap rasters for a given scenario in their respective subdirectories."
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        dest="output_filename",
        help="Destination geotiff file for results."
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

    raster_sum(
        args.rasters_directory,
        args.output_filename,
        args.processes_count
    )

if __name__ == "__main__":
    main()
