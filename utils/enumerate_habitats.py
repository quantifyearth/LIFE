import argparse
import logging
from functools import partial
from multiprocessing import Pool, cpu_count
from typing import Set

from yirgacheffe.layers import RasterLayer  # type: ignore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')

BLOCKSIZE = 512

def enumerate_subset(
    habitat_path: str,
    offset: int,
) -> Set[int]:
    with RasterLayer.layer_from_file(habitat_path) as habitat_map:
        blocksize = min(BLOCKSIZE, habitat_map.window.ysize - offset)
        data = habitat_map.read_array(0, offset, habitat_map.window.xsize, blocksize)
        values = data.flatten()
        res = set(values)
    return res

def enumerate_terrain_types(
    habitat_path: str
) -> Set[int]:
    with RasterLayer.layer_from_file(habitat_path) as habitat_map:
        ysize = habitat_map.window.ysize
    blocks = range(0, ysize, BLOCKSIZE)
    logger.info("Enumerating habitat classes in raster...")
    with Pool(processes=int(cpu_count() / 2)) as pool:
        sets = pool.map(partial(enumerate_subset, habitat_path), blocks)
    superset = set()
    for s in sets:
        superset.update(s)
    logger.info(superset)

def main() -> None:
    parser = argparse.ArgumentParser(description="Downsample habitat map to raster per terrain type.")
    parser.add_argument(
        '--map',
        type=str,
        help="Initial habitat.",
        required=True,
        dest="habitat_path"
    )
    args = parser.parse_args()

    enumerate_terrain_types(
        args.habitat_path,
    )

if __name__ == "__main__":
    main()
