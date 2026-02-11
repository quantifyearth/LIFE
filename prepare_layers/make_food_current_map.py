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
from yirgacheffe.layers import RasterLayer
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
    If we remove one time of agricultural land but expand another, keep them in the same area where possible.

    One thing we know is that reductions are always achievable, as the difference is generated as
    (GAEZ and HYDE - Jung), so if we have a negative value the initial state is enough to cover that.
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
        else:
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
    lcc_code: int,
    diff: float,
    lcc_data_map: dict[int,np.ndarray],
) -> None:
    assert diff >= 0

    agri_raster = lcc_data_map[lcc_code]

    # Find areas we can put the new data. This is anywhere we don't already
    # have agricultural land, and other places unlikely to be converted (cities, lakes, etc.)
    # We know that there should be no partial cells involving crop/pasture at this stage
    # because of the balancing we did initially.
    excluded_codes = [CROP_CODE, PASTURE_CODE] + PRESERVE_CODES
    eligible_mask = np.ones_like(agri_raster, dtype=bool)
    for excluded_code in excluded_codes:
        if excluded_code in lcc_data_map:
            eligible_mask &= (lcc_data_map[excluded_code] == 0)

    eligible_count = eligible_mask.sum()
    if eligible_count == 0:
        return

    # Calculate capacity
    total_cells = eligible_mask.size
    eligible_fraction = eligible_count / total_cells

    if eligible_fraction == 0:
        return

    per_cell_addition = diff / eligible_fraction
    per_cell_addition = min(per_cell_addition, 1.0)

    agri_raster[eligible_mask] += per_cell_addition
    for lcc, lcc_data in lcc_data_map.items():
        if lcc == lcc_code:
            continue
        lcc_data[eligible_mask & (lcc_data > 0)] -= per_cell_addition

def process_tile(
    current_maps: dict[int,yg.layers.RasterLayer],
    pnv: yg.layers.RasterLayer,
    tile: TileInfo,
) -> dict[int,np.ndarray]:

    lcc_data_map = {
        lcc: current_map.nan_to_num().read_array(tile.x_position, tile.y_position, tile.width, tile.height)
        for lcc, current_map in current_maps.items()
    }

    if np.isnan(tile.crop_target) and np.isnan(tile.pasture_target):
        return lcc_data_map

    for current in current_maps.values():
        assert current.map_projection == pnv.map_projection
        assert current.area == pnv.area

    pnv_data = None

    if not np.isnan(tile.crop_target):
        crop_data = lcc_data_map[CROP_CODE]
        crop_diff = tile.crop_target - (crop_data.sum() / crop_data.size)
    else:
        crop_diff = 0
    if not np.isnan(tile.pasture_target):
        pasture_data = lcc_data_map[PASTURE_CODE]
        pasture_diff = tile.pasture_target - (pasture_data.sum() / pasture_data.size)
    else:
        pasture_diff = 0
    if (crop_diff == 0) and (pasture_diff == 0):
        return lcc_data_map

    crop_diff, pasture_diff = balance_crop_and_pasture_differences(
        crop_diff,
        pasture_diff,
        lcc_data_map,
    )

    # Order the changes by removals first then additions. In random sampling this was
    # important, but not so with a fractional approach. However, we need some consistent
    # ordering so we leave this in for consistency.
    diffs = [
        (crop_diff, CROP_CODE),
        (pasture_diff, PASTURE_CODE),
    ]
    diffs.sort(key=lambda a: a[0])

    for diff_value, habitat_code in diffs:
        if diff_value == 0 or np.isnan(diff_value):
            continue

        if diff_value < 0:
            if pnv is None:
                pnv = pnv.read_array(tile.x_position, tile.y_position, tile.width, tile.height)
            remove_land_cover(habitat_code, diff_value, pnv_data, lcc_data_map)
        else:
            add_land_cover(habitat_code, diff_value, lcc_data_map)

    return lcc_data_map

def process_tile_concurrently(
    current_lvl1_path: Path,
    pnv_path: Path,
    input_queue: Queue,
    result_queue: Queue,
) -> None:
    current_maps = {
        int(filename.stem.split('_')[1]): yg.read_raster(filename) for filename in current_lvl1_path.glob("lcc_*.tif")
    }
    reference_layer = next(iter(current_maps.values()))
    with yg.read_raster_like(pnv_path, reference_layer, yg.ResamplingMethod.Nearest) as pnv:
        pnv.set_window_for_intersection(reference_layer.area)
        while True:
            tile : TileInfo | None = input_queue.get()
            if tile is None:
                break

            res = process_tile(current_maps, pnv, tile)
            result_queue.put((tile, {lcc: data.tobytes() for lcc, data in res.items() }))

    result_queue.put(None)

def build_tile_list(
    current_lvl1_path: Path,
    crop_adjustment_path: Path,
    pasture_adjustment_path: Path,
) -> list[TileInfo]:
    tiles = []

    with yg.read_rasters(current_lvl1_path.glob("*.tif")) as current:
        current_dimensions = current.window.xsize, current.window.ysize
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
    current_lvl1_path: Path,
    output_path: Path,
    result_queue: Queue,
    sentinal_count: int,
) -> None:
    os.makedirs(output_path, exist_ok=True)

    original_filenames = list(current_lvl1_path.glob("lcc_*.tif"))
    current_maps = {
        int(filename.stem.split('_')[1]): yg.read_raster(filename).nan_to_num() for filename in original_filenames
    }
    new_maps = {}
    with yg.read_raster(original_filenames[0]) as example:
        for filename in original_filenames:
            lcc = int(filename.stem.split('_')[1])
            outputname = output_path / f"lcc_{lcc}.tif"
            new_maps[lcc] = RasterLayer.empty_raster_layer_like(
                example,
                filename=outputname,
                nodata=0.0,
                threads=16,
                sparse=True,
            )
        dtype = example.read_array(0, 0, 1, 1).dtype

    count = 0
    while True:
        result : tuple[TileInfo,bytearray | None] | None = result_queue.get()
        if result is None:
            sentinal_count -= 1
            if sentinal_count == 0:
                break
            continue

        count += 1
        tile, rawdata = result
        if rawdata is None:
            for lcc in current_maps.keys():
                data = current_maps[lcc].read_array(tile.x_position, tile.y_position, tile.width, tile.height)
                band = new_maps[lcc]._dataset.GetRasterBand(1)
                band.WriteArray(data, tile.x_position, tile.y_position)
        else:
            for lcc in current_maps.keys():
                n = np.frombuffer(rawdata[lcc], dtype=dtype)
                data = np.reshape(n, (tile.height, tile.width))
                band = new_maps[lcc]._dataset.GetRasterBand(1)
                band.WriteArray(data, tile.x_position, tile.y_position)
        if count % 1000 == 0:
            print(f"assembled {count} tiles")


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

def make_food_current_map(
    current_lvl1_path: Path,
    pnv_path: Path,
    crop_adjustment_path: Path,
    pasture_adjustment_path: Path,
    output_path: Path,
    processes_count: int,
) -> None:
    # We'll use a lot of processes which will talk back to the main process, so
    # we need to adjust the ulimit, which is quite low by default
    _, max_fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (max_fd_limit, max_fd_limit))

    os.makedirs(output_path.parent, exist_ok=True)

    source_queue = multiprocessing.Queue(maxsize=1000)
    result_queue = multiprocessing.Queue(maxsize=1000)

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

@snakemake_compatible(mapping={
    "current_lvl1_path": "input.jung",
    "pnv_path": "input.pnv",
    "crop_adjustment_path": "input.crop",
    "pasture_adjustment_path": "input.pasture",
    "processes_count": "threads",
    "output_path": "output.rasters",
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
        "-j",
        type=int,
        required=False,
        default=cpu_count() // 2,
        dest="processes_count",
        help="Number of concurrent threads to use."
    )
    args = parser.parse_args()

    make_food_current_map(
        args.current_lvl1_path,
        args.pnv_path,
        args.crop_adjustment_path,
        args.pasture_adjustment_path,
        args.output_path,
        args.processes_count,
    )

if __name__ == "__main__":
    main()
