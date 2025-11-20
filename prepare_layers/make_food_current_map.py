import argparse
import math
import os
import resource
import sys
import time
from pathlib import Path
from multiprocessing import Manager, Process, cpu_count
from queue import Queue
from typing import NamedTuple

import numpy as np
import yirgacheffe as yg
from yirgacheffe.layers import RasterLayer

NULL_CODE = 0
CROP_CODE = 1401
PASTURE_CODE = 1402
# Codes not to touch. We're currently working at Level 1 except for artificial which is level 2
PRESERVE_CODES = [600, 700, 900, 1000, 1100, 1200, 1300, 1405]

class TileInfo(NamedTuple):
    """Info about a tile to process"""
    x_position : int
    y_position : int
    width : int
    height : int
    crop_diff : float
    pasture_diff : float

def process_tile(
    current: yg.layers.RasterLayer,
    pnv: yg.layers.RasterLayer,
    tile: TileInfo,
    random_seed: int,
) -> np.ndarray:

    rng = np.random.default_rng(random_seed)

    data = current.read_array(tile.x_position, tile.y_position, tile.width, tile.height)

    diffs = [
        (tile.crop_diff, CROP_CODE),
        (tile.pasture_diff, PASTURE_CODE),
    ]
    diffs.sort(key=lambda a: a[0])

    # Ordered so removes will come first
    for diff_value, habitat_code in diffs:
        if np.isnan(diff_value):
            continue
        required_points = math.floor(data.shape[0] * data.shape[1] * math.fabs(diff_value))
        if required_points == 0:
            continue

        if diff_value > 0:
            valid_mask = ~np.isin(data, [CROP_CODE, PASTURE_CODE] + PRESERVE_CODES)
        else:
            valid_mask = data == habitat_code

        valid_locations = valid_mask.nonzero()
        possible_points = len(valid_locations[0])
        if possible_points == 0:
            continue
        required_points = min(required_points, possible_points)

        selected_locations = rng.choice(
            len(valid_locations[0]),
            size=required_points,
            replace=False
        )
        rows = valid_locations[0][selected_locations]
        cols = valid_locations[1][selected_locations]
        if diff_value > 0:
            data[rows, cols] = habitat_code
        else:
            for y, x in zip(rows, cols):
                absolute_x = x + tile.x_position
                absolute_y = y + tile.y_position
                lat, lng = current.latlng_for_pixel(absolute_x, absolute_y)
                pnv_x, pnv_y = pnv.pixel_for_latlng(lat, lng)
                val = pnv.read_array(pnv_x, pnv_y, 1, 1)[0][0]
                data[y][x] = val

    return data

def process_tile_concurrently(
    current_lvl1_path: Path,
    pnv_path: Path,
    input_queue: Queue,
    result_queue: Queue,
) -> None:
    with yg.read_raster(current_lvl1_path) as current:
        with yg.read_raster(pnv_path) as pnv:
            while True:
                job : tuple[TileInfo, int] | None = input_queue.get()
                if job is None:
                    break
                tile, seed = job
                if np.isnan(tile.crop_diff) and np.isnan(tile.pasture_diff):
                    result_queue.put((tile, None))
                else:
                    data = process_tile(current, pnv, tile, seed)
                    result_queue.put((tile, data.tobytes()))

    result_queue.put(None)

def build_tile_list(
    current_lvl1_path: Path,
    crop_adjustment_path: Path,
    pasture_adjustment_path: Path,
) -> list[TileInfo]:
    tiles = []
    with yg.read_raster(current_lvl1_path) as current:
        current_dimensions = current.window.xsize, current.window.ysize
    with yg.read_raster(crop_adjustment_path) as crop_diff:
        with yg.read_raster(pasture_adjustment_path) as pasture_diff:
            assert crop_diff.window == pasture_diff.window
            diff_dimensions = crop_diff.window.xsize, crop_diff.window.ysize

            x_scale = current_dimensions[0] / diff_dimensions[0]
            y_scale = current_dimensions[1] / diff_dimensions[1]

            x_steps = [round(i * x_scale) for i in range(diff_dimensions[0])]
            x_steps.append(current_dimensions[0])
            y_steps = [round(i * y_scale) for i in range(diff_dimensions[1])]
            y_steps.append(current_dimensions[1])

            for y in range(crop_diff.window.ysize):
                crop_row = crop_diff.read_array(0, y, crop_diff.window.xsize, 1)
                pasture_row = pasture_diff.read_array(0, y, pasture_diff.window.xsize, 1)
                for x in range(crop_diff.window.xsize):
                    tiles.append(TileInfo(
                        x_steps[x],
                        y_steps[y],
                        (x_steps[x+1] - x_steps[x]),
                        (y_steps[y+1] - y_steps[y]),
                        crop_row[0][x],
                        pasture_row[0][x],
                    ))
    return tiles

def assemble_map(
    current_lvl1_path: Path,
    output_path: Path,
    result_queue: Queue,
    sentinal_count: int,
) -> None:

    with yg.read_raster(current_lvl1_path) as current:
        dtype = current.read_array(0, 0, 1, 1).dtype
        with RasterLayer.empty_raster_layer_like(current, filename=output_path) as output:

            # A cheat as we don't have a neat API for this on yirgacheffe yet
            band = output._dataset.GetRasterBand(1) # pylint: disable=W0212

            while True:
                result : tuple[TileInfo,bytearray | None] | None = result_queue.get()
                if result is None:
                    sentinal_count -= 1
                    if sentinal_count == 0:
                        break
                    continue

                tile, rawdata = result
                if rawdata is None:
                    data = current.read_array(tile.x_position, tile.y_position, tile.width, tile.height)
                else:
                    n = np.frombuffer(rawdata, dtype=dtype)
                    data = np.reshape(n, (tile.height, tile.width))

                band.WriteArray(data, tile.x_position, tile.y_position)


def pipeline_source(
    current_lvl1_path: Path,
    crop_adjustment_path: Path,
    pasture_adjustment_path: Path,
    source_queue: Queue,
    sentinal_count: int,
    random_seed: int,
) -> None:
    rng = np.random.default_rng(random_seed)

    tiles = build_tile_list(
        current_lvl1_path,
        crop_adjustment_path,
        pasture_adjustment_path,
    )
    seeds = rng.integers(2**63, size=len(tiles))
    for tile, seed in zip(tiles, seeds):
        source_queue.put((tile, seed))
    for _ in range(sentinal_count):
        source_queue.put(None)

def make_food_current_map(
    current_lvl1_path: Path,
    pnv_path: Path,
    crop_adjustment_path: Path,
    pasture_adjustment_path: Path,
    random_seed: int,
    output_path: Path,
    processes_count: int,
) -> None:
    # We'll use a lot of processes which will talk back to the main process, so
    # we need to adjust the ulimit, which is quite low by default
    _, max_fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (max_fd_limit, max_fd_limit))
    print(f"Set fd limit to {max_fd_limit}")

    os.makedirs(output_path.parent, exist_ok=True)

    with Manager() as manager:
        source_queue = manager.Queue()
        result_queue = manager.Queue()

        workers = [Process(target=process_tile_concurrently, args=(
            current_lvl1_path,
            pnv_path,
            source_queue,
            result_queue,
        )) for _ in range(processes_count)]
        for worker_process in workers:
            worker_process.start()

        source_worker = Process(target=pipeline_source, args=(
            current_lvl1_path,
            crop_adjustment_path,
            pasture_adjustment_path,
            source_queue,
            processes_count,
            random_seed,
        ))
        source_worker.start()

        assemble_map(
            current_lvl1_path,
            output_path,
            result_queue,
            processes_count,
        )

        processes = workers
        processes.append(source_worker)
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

def main() -> None:
    parser = argparse.ArgumentParser(description="Build the food current map")
    parser.add_argument(
        "--current_lvl1",
        type=Path,
        required=True,
        help="Path of lvl1 current map",
        dest="current_lvl1_path",
    )
    parser.add_argument(
        '--pnv',
        type=Path,
        help='Path of PNV map',
        required=True,
        dest='pnv_path',
    )
    parser.add_argument(
        "--crop_diff",
        type=Path,
        required=True,
        help="Path of adjustment for crop diff",
        dest="crop_adjustment_path",
    )
    parser.add_argument(
        "--pasture_diff",
        type=Path,
        required=True,
        help="Path of adjustment for pasture diff",
        dest="pasture_adjustment_path",
    )
    parser.add_argument(
        "--seed",
        type=int,
        required=True,
        help="Seed the random number generator",
        dest="seed",
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Path of food current raster',
        required=True,
        dest='output_path',
    )
    parser.add_argument(
        "-j",
        type=int,
        required=False,
        default=round(cpu_count() / 1),
        dest="processes_count",
        help="Number of concurrent threads to use."
    )
    args = parser.parse_args()

    make_food_current_map(
        args.current_lvl1_path,
        args.pnv_path,
        args.crop_adjustment_path,
        args.pasture_adjustment_path,
        args.seed,
        args.output_path,
        args.processes_count,
    )

if __name__ == "__main__":
    main()
