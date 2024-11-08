import argparse
import functools
import operator
import os
import random
import sys
from multiprocessing import Pool, cpu_count

import matplotlib.pyplot as plt
import numpy as np
from yirgacheffe.layers import RasterLayer

def filter_data(chunks):
    a_chunk, b_chunk = chunks
    res = []
    for a, b in zip(a_chunk, b_chunk):
        if np.isnan(a) or np.isnan(b):
            continue
        if a == 0.0 and b == 0.0:
            continue
        res.append((a, b))
    return res

def regression_plot(
    a_path: str,
    b_path: str,
    output_path: str,
) -> None:
    output_dir, _ = os.path.split(output_path)
    os.makedirs(output_dir, exist_ok=True)

    with RasterLayer.layer_from_file(a_path) as a_layer:
        with RasterLayer.layer_from_file(b_path) as b_layer:
            if a_layer.pixel_scale != b_layer.pixel_scale:
                sys.exit("GeoTIFFs have different pixel scale")
            if a_layer.area != b_layer.area:
                sys.exit("GeoTIFFs have different spatial coordinates")
            if a_layer.window != b_layer.window:
                sys.exit("GeoTIFFs have different pixel dimensions")

            a_pixels = a_layer.read_array(0, 0, a_layer.window.xsize, a_layer.window.ysize)
            b_pixels = b_layer.read_array(0, 0, b_layer.window.xsize, b_layer.window.ysize)

    with Pool(processes=cpu_count() // 2) as pool:
        filtered_chunk_pairs = pool.map(filter_data, zip(a_pixels, b_pixels))
        filtered_pairs = functools.reduce(operator.iconcat, filtered_chunk_pairs, [])
        sampled_pairs = random.sample(filtered_pairs, len(filtered_pairs) // 10)
        a_filtered, b_filtered = zip(*sampled_pairs)

    # m, b = np.polyfit(a_filtered, b_filtered, 1)

    _fig, ax = plt.subplots()
    ax.scatter(x=a_filtered, y=b_filtered, marker=",")
    plt.xlabel(os.path.basename(a_path))
    plt.ylabel(os.path.basename(b_path))
    plt.savefig(output_path)

def main() -> None:
    parser = argparse.ArgumentParser(description="Generates a scatter plot comparing two GeoTIFFs.")
    parser.add_argument(
        "--a",
        type=str,
        required=True,
        dest="a",
        help="First GeoTIFF"
    )
    parser.add_argument(
        "--b",
        type=str,
        required=True,
        dest="b",
        help="Second GeoTIFF"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        dest="output",
        help="Destination png file for results."
    )
    args = parser.parse_args()

    regression_plot(
        args.a,
        args.b,
        args.output,
    )

if __name__ == "__main__":
    main()

