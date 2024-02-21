import sys

import numpy as np
from yirgacheffe.layers import RasterLayer

RESTORE_MAP = "/maps/results/global_analysis/rasters/area_1_arc/area/diff_restore_area.tif"

def main():
    try:
        input_layer = sys.argv[1]
        output_layer = sys.argv[2]
    except IndexError:
        print(f"Usage: {sys.argv[0]} [INPUT LAYER] [OUTPUT LAYER]", file=sys.stderr)
        sys.exit(1)
    
    area_restore = RasterLayer.layer_from_file(RESTORE_MAP)    
    inlayer = RasterLayer.layer_from_file(input_layer)

    intersection = RasterLayer.find_intersection([area_restore, inlayer])
    inlayer.set_window_for_intersection(intersection)
    area_restore.set_window_for_intersection(intersection)
    result = RasterLayer.empty_raster_layer_like(inlayer, filename=output_layer, nodata=float('nan'))

    area_restore_filter = area_restore.numpy_apply(lambda c: np.where(c < 1e4, 0, c)) / 1e4
    filtered_layer = inlayer.numpy_apply(lambda il, af: np.where(af != 0, il, 0), area_restore_filter)
    scaled_filtered_layer = filtered_layer / area_restore_filter
    scaled_filtered_layer.save(result)




if __name__ == "__main__":
    main()
