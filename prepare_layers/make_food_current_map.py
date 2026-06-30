import argparse
import multiprocessing
import os
import resource
import sys
import time
from pathlib import Path
from multiprocessing import Process, cpu_count
from queue import Queue
from typing import NamedTuple
from osgeo import gdal

import numpy as np
import yirgacheffe as yg
from snakemake_argparse_bridge import snakemake_compatible # type: ignore

gdal.SetCacheMax(4 * 1024 * 1024 * 1024)

NULL_CODE = 0
CROP_CODE = 1401
PASTURE_CODE = 1402
# Codes not to touch. We're currently working at Level 1 except for artificial which is level 2
PRESERVE_CODES = [600, 700, 900, 1000, 1100, 1200, 1300, 1405]

# PNV codes
# array([ 100,  200,  300,  400,  500,  600,  800,  900, 1100, 1200], dtype=uint16)

class TileInfo(NamedTuple):
    """Info about a tile to process"""
    x_position : int
    y_position : int
    width : int
    height : int
    crop_target : float
    pasture_target : float

def balance_crop_and_pasture_differences(
    crop_diff: float,
    pasture_diff: float,
    lcc_data_map: dict[int,np.ndarray],
) -> tuple[float,float]:
    """
    If we remove one type of agricultural land but expand another, keep them in the same area where possible.
    """
    # Either both are a reduction or both an increase, or at least one is null, so no
    # balancing required.
    if crop_diff * pasture_diff >= 0:
        return crop_diff, pasture_diff

    # If balanced they will cancel each other out, otherwise
    # we will move the smaller difference from one to the other.
    transfer_amount = min(abs(crop_diff), abs(pasture_diff))

    if crop_diff > 0:
        # Crop increasing, pasture decreasing
        src_lcc, dst_lcc = PASTURE_CODE, CROP_CODE
    else:
        # Pasture increasing, crop decreasing
        src_lcc, dst_lcc = CROP_CODE, PASTURE_CODE

    src_raster = lcc_data_map[src_lcc]
    dst_raster = lcc_data_map[dst_lcc]

    total_cells = src_raster.size

    transfer_mask = src_raster > 0
    src_cells = src_raster.sum()
    if src_cells == 0:
        if crop_diff > 0:
            raise ValueError(f"not cells in pasture {src_raster.sum()}, but pasture diff is -ve {pasture_diff}")
        raise ValueError(f"not cells in crop {src_raster.sum()}, but crop diff is -ve {crop_diff}")

    src_coverage = src_raster.sum() / total_cells

    # Per-cell reduction factor: what fraction of each cell's current value to move
    # This is safe because our simplifying assumption guarantees transfer <= source_coverage
    per_cell_factor = transfer_amount / src_coverage

    transferred = transfer_mask * per_cell_factor
    src_raster -= transferred
    dst_raster += transferred

    new_crop_diff = crop_diff + transfer_amount * np.sign(pasture_diff)
    new_pasture_diff = pasture_diff + transfer_amount * np.sign(crop_diff)
    return new_crop_diff, new_pasture_diff

def remove_land_cover(
    lcc_code: int,
    diff: float,
    pnv: np.ndarray,
    lcc_data_map: dict[int,np.ndarray],
) -> None:
    assert diff <= 0
    diff = abs(diff)

    agri_raster = lcc_data_map[lcc_code]
    removal_mask = agri_raster > 0

    current_coverage = agri_raster.sum() / agri_raster.size
    if current_coverage == 0:
        return

    per_cell_fraction = min(diff / current_coverage, 1.0)

    removed_grid = agri_raster * (removal_mask * per_cell_fraction)
    agri_raster -= removed_grid

    # Reallocate to PNV classes - note we assume this does not include the agricultural classes
    # so as to not undo what we just did!
    for lcc, lcc_data in lcc_data_map.items():
        pnv_match = (pnv == lcc) & removal_mask
        lcc_data[pnv_match] += removed_grid[pnv_match]

def add_land_cover(
    eligible_mask: np.ndarray,
    diffs: list[tuple[float, int]],
    lcc_data_map: dict[int,np.ndarray],
) -> None:

    # Calculate capacity
    eligible_count = eligible_mask.sum()
    if eligible_count == 0:
        return
    total_cells = eligible_mask.size
    eligible_fraction = eligible_count / total_cells
    if eligible_fraction == 0:
        return

    total_addition = 0
    for diff, lcc_code in diffs:
        assert 0 <= diff <= 1

        agri_raster = lcc_data_map[lcc_code]

        per_cell_addition = diff / eligible_fraction
        per_cell_addition = min(per_cell_addition, 1.0)
        total_addition += per_cell_addition

        agri_raster[eligible_mask] += per_cell_addition

    # Remove from the other land cover classes. This assumes that coming into this
    # stage the LCC pixels are:
    # * only non-zero in a single layer
    # * only starting at 100%
    for lcc, lcc_data in lcc_data_map.items():
        if lcc in [CROP_CODE, PASTURE_CODE] + PRESERVE_CODES:
            continue
        lcc_data[eligible_mask & (lcc_data > 0)] -= total_addition
        lcc_data[eligible_mask] = np.maximum(lcc_data[eligible_mask], 0.0)

def process_tile(
    current_maps: dict[int,yg.YirgacheffeLayer],
    pnv: yg.YirgacheffeLayer,
    tile: TileInfo,
) -> dict[int,np.ndarray]:
    lcc_data_map = {
        lcc: current_map.read_array(tile.x_position, tile.y_position, tile.width, tile.height)
        for lcc, current_map in current_maps.items()
    }

    if np.isnan(tile.crop_target) and np.isnan(tile.pasture_target):
        return lcc_data_map

    for current in current_maps.values():
        assert current.map_projection == pnv.map_projection
        assert current.area == pnv.area

    if not np.isnan(tile.crop_target):
        crop_data = lcc_data_map[CROP_CODE]
        crop_diff = tile.crop_target - (crop_data.sum() / crop_data.size)
        assert not np.isnan(crop_diff)
    else:
        crop_diff = 0
    if not np.isnan(tile.pasture_target):
        pasture_data = lcc_data_map[PASTURE_CODE]
        pasture_diff = tile.pasture_target - (pasture_data.sum() / pasture_data.size)
        assert not np.isnan(pasture_diff)
    else:
        pasture_diff = 0
    if (crop_diff == 0) and (pasture_diff == 0):
        return lcc_data_map

    crop_diff, pasture_diff = balance_crop_and_pasture_differences(
        crop_diff,
        pasture_diff,
        lcc_data_map,
    )

    # We first do all the removals and then the additions
    diffs = [
        (crop_diff, CROP_CODE),
        (pasture_diff, PASTURE_CODE),
    ]
    removals = [x for x in diffs if x[0] < 0]
    additions = [x for x in diffs if x[0] > 0]

    pnv_data = None # lazy PNV load as it's expensive
    for diff_value, habitat_code in removals:
        if pnv_data is None:
            pnv_data = pnv.read_array(tile.x_position, tile.y_position, tile.width, tile.height)
        remove_land_cover(habitat_code, diff_value, pnv_data, lcc_data_map)

    # If there's no additions we don't need to make the eligible_mask, and we can go
    # home early.
    if not additions:
        return lcc_data_map

    # Find areas we can put the new data. This is anywhere we don't already
    # have agricultural land, and other places unlikely to be converted (cities, lakes, etc.)
    # We know that there should be no partial cells involving crop/pasture at this stage
    # because of the balancing we did initially.
    excluded_codes = [CROP_CODE, PASTURE_CODE] + PRESERVE_CODES
    eligible_mask = np.ones_like(lcc_data_map[CROP_CODE], dtype=bool)
    for excluded_code in excluded_codes:
        if excluded_code in lcc_data_map:
            eligible_mask &= (lcc_data_map[excluded_code] == 0)

    # There is a risk that the total is not achievable as there is a disagreement between the combined
    # GAEZ/HYDE, Jung, and our PRESERVE_CODES list (that say don't covert urban or rocky land to farmland).
    # As such we need to adjust for what is actually achievable before we make changes.
    available_cells = eligible_mask.sum() / eligible_mask.size
    total_desired_change = sum(x[0] for x in additions)
    if total_desired_change > available_cells:
        # Split what is actually possible proportionally to the original demand
        additions = [
            ((change / total_desired_change) * available_cells, klass)
            for (change, klass) in additions
        ]

    add_land_cover(eligible_mask, additions, lcc_data_map)

    return lcc_data_map


def process_tile_concurrently(
    current_lvl1_path: Path,
    pnv_path: Path,
    input_queue: Queue,
    result_queues: dict[int,Queue],
) -> None:
    current_maps = {
        int(filename.stem.split('_')[1]): yg.read_raster(filename) for filename in current_lvl1_path.glob("lcc_*.tif")
    }
    reference_layer = next(iter(current_maps.values()))
    with yg.read_raster(pnv_path) as pnv:
        pnv.set_window_for_intersection(reference_layer.area)
        while True:
            tile : TileInfo | None = input_queue.get()
            if tile is None:
                break
            res = process_tile(current_maps, pnv, tile)
            for lcc, data in res.items():
                result_queues[lcc].put((tile, data.tobytes()))
    for queue in result_queues.values():
        queue.put(None)

def build_tile_list(
    current_lvl1_path: Path,
    crop_adjustment_path: Path,
    pasture_adjustment_path: Path,
) -> list[TileInfo]:
    tiles = []

    with yg.read_raster(next(current_lvl1_path.glob("*.tif"))) as example:
        current_dimensions = example.window.xsize, example.window.ysize
    with (
        yg.read_raster(crop_adjustment_path) as crop,
        yg.read_raster(pasture_adjustment_path) as pasture,
    ):
        assert crop.window == pasture.window
        argi_dimensions = crop.window.xsize, crop.window.ysize

        x_scale = current_dimensions[0] / argi_dimensions[0]
        y_scale = current_dimensions[1] / argi_dimensions[1]

        x_steps = [round(i * x_scale) for i in range(argi_dimensions[0])]
        x_steps.append(current_dimensions[0])
        y_steps = [round(i * y_scale) for i in range(argi_dimensions[1])]
        y_steps.append(current_dimensions[1])

        for y in range(crop.window.ysize):
            crop_row = crop.read_array(0, y, crop.window.xsize, 1)
            pasture_row = pasture.read_array(0, y, pasture.window.xsize, 1)
            for x in range(crop.window.xsize):
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
    lcc: int,
    current_lvl1_path: Path,
    output_path: Path,
    result_queue: Queue,
    sentinal_count: int,
) -> None:
    os.makedirs(output_path, exist_ok=True)
    with yg.read_raster(current_lvl1_path / f"lcc_{lcc}.tif") as current_map:
        new_map = yg.layers.RasterLayer.empty_raster_layer_like(
            current_map,
            filename=output_path / f"lcc_{lcc}.tif",
            threads=16,
        )
        dtype = current_map.read_array(0, 0, 1, 1).dtype
    band = new_map._dataset.GetRasterBand(1) # pylint: disable=W0212

    count = 0
    while True:
        result : tuple[TileInfo,bytearray] | None = result_queue.get()
        if result is None:
            sentinal_count -= 1
            if sentinal_count == 0:
                break
            continue

        count += 1
        tile, rawdata = result
        n = np.frombuffer(rawdata, dtype=dtype)
        data = np.reshape(n, (tile.height, tile.width))
        band.WriteArray(data, tile.x_position, tile.y_position)
        if count % 1000 == 0:
            print(f"{lcc}: assembled {count} tiles")

def pipeline_source(
    current_lvl1_path: Path,
    crop_adjustment_path: Path,
    pasture_adjustment_path: Path,
    source_queue: Queue,
    sentinal_count: int,
) -> None:
    tiles = build_tile_list(
        current_lvl1_path,
        crop_adjustment_path,
        pasture_adjustment_path,
    )
    print(f"There are {len(tiles)} tiles")
    for tile in tiles:
        source_queue.put(tile)
    for _ in range(sentinal_count):
        source_queue.put(None)


def get_lcc_list(current_lvl1_path: Path) -> list[int]:
    rasters = current_lvl1_path.glob("*.tif")
    return [int(x.stem.split('_')[1]) for x in rasters]

def make_food_current_map(
    current_lvl1_path: Path,
    pnv_path: Path,
    crop_adjustment_path: Path,
    pasture_adjustment_path: Path,
    output_path: Path,
    processes_count: int,
    sentinel_path: Path | None,
) -> None:
    # We'll use a lot of processes which will talk back to the main process, so
    # we need to adjust the ulimit, which is quite low by default
    _, max_fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (max_fd_limit, max_fd_limit))

    os.makedirs(output_path.parent, exist_ok=True)

    lcc_list = get_lcc_list(current_lvl1_path)
    result_queues: dict[int,multiprocessing.queues.Queue] = {
        lcc: multiprocessing.Queue(maxsize=10) for lcc in lcc_list
    }

    assembly_processes = [
        Process(target=assemble_map, args=(
            lcc,
            current_lvl1_path,
            output_path,
            queue,
            processes_count,
        )) for lcc, queue in result_queues.items()
    ]
    for assembly_worker in assembly_processes:
        assembly_worker.start()

    source_queue: multiprocessing.queues.Queue = multiprocessing.Queue(maxsize=1000)

    workers = [Process(target=process_tile_concurrently, args=(
        current_lvl1_path,
        pnv_path,
        source_queue,
        result_queues,
    )) for _ in range(processes_count)]
    for worker_process in workers:
        worker_process.start()

    source_worker = Process(target=pipeline_source, args=(
        current_lvl1_path,
        crop_adjustment_path,
        pasture_adjustment_path,
        source_queue,
        processes_count,
    ))
    source_worker.start()

    processes = workers + assembly_processes
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

    if sentinel_path:
        sentinel_path.touch()

@snakemake_compatible(mapping={
    "current_lvl1_path": "params.jung_dir",
    "pnv_path": "input.pnv",
    "crop_adjustment_path": "input.crop",
    "pasture_adjustment_path": "input.pasture",
    "processes_count": "threads",
    "output_path": "params.output_dir",
    "sentinel_path": "output.sentinel",
    "parallelism": "threads",
})
def main() -> None:
    parser = argparse.ArgumentParser(description="Build the food current map")
    parser.add_argument(
        "--current_lvl1",
        type=Path,
        required=True,
        help="Path of lvl1 current maps",
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
        "--crop",
        type=Path,
        required=True,
        help="Path of crop area",
        dest="crop_adjustment_path",
    )
    parser.add_argument(
        "--pasture",
        type=Path,
        required=True,
        help="Path of pasture area",
        dest="pasture_adjustment_path",
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Path of food current raster',
        required=True,
        dest='output_path',
    )
    parser.add_argument(
        '--sentinel',
        type=Path,
        required=False,
        dest='sentinel_path',
    )
    parser.add_argument(
        "-j",
        type=int,
        required=False,
        default=cpu_count() // 2,
        dest="parallelism",
        help="Number of concurrent threads to use."
    )
    args = parser.parse_args()

    make_food_current_map(
        args.current_lvl1_path,
        args.pnv_path,
        args.crop_adjustment_path,
        args.pasture_adjustment_path,
        args.output_path,
        args.parallelism,
        args.sentinel_path,
    )

if __name__ == "__main__":
    main()
