from math import ceil
import os
import shutil
import sys
import tempfile

from osgeo import gdal
from yirgacheffe.layers import Layer

def main():
    layers = [Layer.layer_from_file(x) for x in sys.argv[1:]]
    area = Layer.find_union(layers)

    for layer in layers:
        layer.set_window_for_union(area)
    pixel_pitch = layers[0].pixel_scale

    driver = gdal.GetDriverByName('GTiff')
    with tempfile.TemporaryDirectory() as tempdir:
        tmp_filename = os.path.join(tempdir, "results.tif")

        dataset = driver.Create(
            tmp_filename,
            ceil((area.right - area.left) / pixel_pitch[0]),
            ceil((area.top - area.bottom) / (pixel_pitch[1] * -1)),
            1,
            gdal.GDT_Float32,
            []
        )
        if dataset is None:
            print(f"Failed to create {tmp_filename}")
            sys.exit(-1)

        dataset.SetGeoTransform([
            area.left, pixel_pitch[0], 0.0, area.top, 0.0, pixel_pitch[1]
        ])
        dataset.SetProjection(layers[0].projection)

        output_band = dataset.GetRasterBand(1)
        pixel_width = layers[0].window.xsize
        pixel_height = layers[0].window.ysize

        for yoffset in range(pixel_height):
            first = layers[0].read_array(0, yoffset, pixel_width, 1)
            for other_layer in layers[1:]:
                other = other_layer.read_array(0, yoffset, pixel_width, 1)
                first = first + other
            # Uncomment the below line to help see everything in QGIS
            # first = numpy.logical_and(first > 0.0, True)
            output_band.WriteArray(first, 0, yoffset)

        # Force a close on the dataset and move it to the final location
        del dataset
        shutil.move(tmp_filename, "result.tif")


if __name__ == "__main__":
    main()
