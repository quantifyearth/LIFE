# Endemism is the geometric mean of the proportion of how much each cell contributes to a species total AoH.
# Uses the trick from https://stackoverflow.com/questions/43099542/python-easy-way-to-do-geometric-mean-in-python
# for calculating the geometric mean with less risk of overflow

import argparse
import os
import sys
import tempfile
import time
from glob import glob
from multiprocessing import Manager, Process, Queue, cpu_count

import numpy as np
from osgeo import gdal
from yirgacheffe.layers import RasterLayer

def stage_1_worker(
    filename: str,
    result_dir: str,
    input_queue: Queue,
) -> None:
    output_tif = os.path.join(result_dir, filename)

    merged_result = None

    while True:
        raster_paths = input_queue.get()
        if raster_paths is None:
            break

        rasters = [RasterLayer.layer_from_file(x) for x in raster_paths]

        match len(rasters):
            case 2:
                union = RasterLayer.find_union(rasters)
                for r in rasters:
                    r.set_window_for_union(union)

                aoh1 = rasters[0].sum()
                if aoh1 > 0.0:
                    season1 = rasters[0].numpy_apply(
                        lambda a: np.nan_to_num(np.log(np.where(a == 0, np.nan, a) / aoh1))
                    )
                else:
                    season1 = None
                aoh2 = rasters[1].sum()
                if aoh2 > 0.0:
                    season2 = rasters[1].numpy_apply(
                        lambda a: np.nan_to_num(np.log(np.where(a == 0, np.nan, a) / aoh2))
                    )
                else:
                    season2 = None

                match season1, season2:
                    case None, None:
                        continue
                    case a, None:
                        combined = a
                    case None, b:
                        combined = b
                    case _, _:
                        combined = season1.numpy_apply(lambda a, b: np.where(a > b, a, b), season2)

                partial = RasterLayer.empty_raster_layer_like(rasters[0], datatype=gdal.GDT_Float64)
                combined.save(partial)
            case 1:
                aoh = rasters[0].sum()
                if aoh > 0.0:
                    partial = rasters[0].numpy_apply(
                        lambda a: np.nan_to_num(np.log(np.where(a == 0, np.nan, a) / aoh))
                    )
                else:
                    continue
            case _:
                raise ValueError("too many seasons")

        if merged_result is None:
            if len(rasters) > 1:
                merged_result = partial
            else:
                merged_result = RasterLayer.empty_raster_layer_like(rasters[0], datatype=gdal.GDT_Float64)
                partial.save(merged_result)
        else:
            merged_result.reset_window()
            if len(rasters) > 1:
                union = RasterLayer.find_union([merged_result, partial])
                partial.set_window_for_union(union)
            else:
                union = RasterLayer.find_union([merged_result, rasters[0]])
                rasters[0].set_window_for_union(union)
            merged_result.set_window_for_union(union)

            merged = partial + merged_result
            temp = RasterLayer.empty_raster_layer_like(merged_result)
            merged.save(temp)
            merged_result = temp

    if merged_result is not None:
        final = RasterLayer.empty_raster_layer_like(merged_result, filename=output_tif)
        merged_result.save(final)

def stage_2_worker(
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
                merged_result = RasterLayer.empty_raster_layer_like(partial_raster)
                cleaned_raster = partial_raster.numpy_apply(
                    lambda chunk: np.nan_to_num(chunk, copy=False, nan=0.0)
                )
                cleaned_raster.save(merged_result)
            else:
                merged_result.reset_window()

                union = RasterLayer.find_union([merged_result, partial_raster])
                merged_result.set_window_for_union(union)
                partial_raster.set_window_for_union(union)

                calc = merged_result + partial_raster
                temp = RasterLayer.empty_raster_layer_like(merged_result)
                calc.save(temp)
                merged_result = temp

    if merged_result:
        final = RasterLayer.empty_raster_layer_like(merged_result, filename=output_tif)
        merged_result.save(final)

def endemism(
    aohs_dir: str,
    species_richness_path: str,
    output_path: str,
    processes_count: int
) -> None:
    output_dir, _ = os.path.split(output_path)
    os.makedirs(output_dir, exist_ok=True)

    aohs = glob("**/*.tif", root_dir=aohs_dir)
    print(f"We fould {len(aohs)} AoH rasters")

    species_rasters = {}
    for raster_path in aohs:
        speciesid = os.path.splitext(os.path.basename(raster_path))[0]
        full_path = os.path.join(aohs_dir, raster_path)
        try:
            species_rasters[speciesid].add(full_path)
        except KeyError:
            species_rasters[speciesid] = set([full_path])

    with tempfile.TemporaryDirectory() as tempdir:
        with Manager() as manager:
            source_queue = manager.Queue()

            workers = [Process(target=stage_1_worker, args=(
                f"{index}.tif",
                tempdir,
                source_queue
            )) for index in range(processes_count)]
            for worker_process in workers:
                worker_process.start()

            for raster in species_rasters.items():
                source_queue.put(raster)
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
            single_worker = Process(target=stage_2_worker, args=(
                "summed_proportion.tif",
                output_dir,
                source_queue
            ))
            single_worker.start()
            nextfiles = [os.path.join(tempdir, x) for x in glob("*.tif", root_dir=tempdir)]
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

        with RasterLayer.layer_from_file(species_richness_path) as species_richness:
            with RasterLayer.layer_from_file(os.path.join(output_dir, "summed_proportion.tif")) as summed_proportion:

                intersection = RasterLayer.find_intersection([summed_proportion, species_richness])
                summed_proportion.set_window_for_intersection(intersection)
                species_richness.set_window_for_intersection(intersection)

                cleaned_species_richness = species_richness.numpy_apply(lambda a: np.where(a > 0, a, np.nan))

                with RasterLayer.empty_raster_layer_like(
                    summed_proportion,
                    filename=output_path,
                    nodata=np.nan
                ) as result:
                    calc = summed_proportion.numpy_apply(lambda a, b: np.exp(a / b), cleaned_species_richness)
                    calc.parallel_save(result)


def main() -> None:
    parser = argparse.ArgumentParser(description="Calculate species richness")
    parser.add_argument(
        "--aohs_folder",
        type=str,
        required=True,
        dest="aohs",
        help="Folder containing set of AoHs"
    )
    parser.add_argument(
        "--species_richness",
        type=str,
        required=True,
        dest="species_richness",
        help="GeoTIFF containing species richness"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        dest="output",
        help="Destination GeoTIFF file for results."
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

    endemism(
        args.aohs,
        args.species_richness,
        args.output,
        args.processes_count
    )

if __name__ == "__main__":
    main()
