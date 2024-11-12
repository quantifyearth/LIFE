import argparse
import math

import numpy as np
from osgeo import gdal
from yirgacheffe.window import Area, PixelScale
from yirgacheffe.layers import RasterLayer

# Taken from https://gis.stackexchange.com/questions/127165/more-accurate-way-to-calculate-area-of-rasters
def area_of_pixel(pixel_size, center_lat):
    """Calculate m^2 area of a wgs84 square pixel.

    Adapted from: https://gis.stackexchange.com/a/127327/2397

    Parameters:
        pixel_size (float): length of side of pixel in degrees.
        center_lat (float): latitude of the center of the pixel. Note this
            value +/- half the `pixel-size` must not exceed 90/-90 degrees
            latitude or an invalid area will be calculated.

    Returns:
        Area of square pixel of side length `pixel_size` centered at
        `center_lat` in m^2.

    """
    a = 6378137  # meters
    b = 6356752.3142  # meters
    e = math.sqrt(1 - (b/a)**2)
    area_list = []
    for f in [center_lat+pixel_size/2, center_lat-pixel_size/2]:
        zm = 1 - e*math.sin(math.radians(f))
        zp = 1 + e*math.sin(math.radians(f))
        area_list.append(
            math.pi * b**2 * (
                math.log(zp/zm) / (2*e) +
                math.sin(math.radians(f)) / (zp*zm)))
    return pixel_size / 360. * (area_list[0] - area_list[1])

def make_area_map(
    pixel_scale: float,
    output_path: str
) -> None:
    pixels = [0,] * math.floor(90.0 / pixel_scale)
    for i in range(len(pixels)):  # pylint: disable=C0200
        y = (i + 0.5) * pixel_scale
        area = area_of_pixel(pixel_scale, y)
        pixels[i] = area

    allpixels = np.rot90(np.array([list(reversed(pixels)) + pixels]))

    area = Area(
        left=math.floor(180 / pixel_scale) * pixel_scale * -1.0,
        right=((math.floor(180 / pixel_scale) - 1) * pixel_scale * -1.0),
        top=(math.floor(90 / pixel_scale) * pixel_scale),
        bottom=(math.floor(90 / pixel_scale) * pixel_scale * -1.0)
    )
    with RasterLayer.empty_raster_layer(
        area,
        PixelScale(pixel_scale, pixel_scale * -1.0),
        gdal.GDT_Float32,
        filename=output_path
    ) as res:
        res._dataset.WriteArray(allpixels, 0, 0)  # pylint: disable=W0212


def main() -> None:
    parser = argparse.ArgumentParser(description="Downsample habitat map to raster per terrain type.")
    parser.add_argument(
        "--scale",
        type=float,
        required=True,
        dest="pixel_scale",
        help="Output pixel scale value."
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        dest="output_path",
        help="Destination file for area raster."
    )
    args = parser.parse_args()

    make_area_map(
        args.pixel_scale,
        args.output_path
    )

if __name__ == "__main__":
    main()
