from collections import namedtuple
from math import ceil, floor
from typing import List, Any, Tuple

import numpy
from osgeo import gdal, ogr


Window = namedtuple('Window', ['xoff', 'yoff', 'xsize', 'ysize'])
Area = namedtuple('Area', ['left', 'top', 'right', 'bottom'])
PixelScale = namedtuple('PixelScale', ['xstep', 'ystep'])

class Layer:
    @staticmethod
    def find_intersection(layers: List) -> Area:
        if not layers:
            raise ValueError("Expected list of layers")

        # This only makes sense (currently) if all layers
        # have the same pixel pitch
        scale = layers[0].pixel_scale
        for layer in layers[1:]:
            if not layer.check_pixel_scale(scale):
                raise ValueError("Not all layers are at the same pixel scale")

        intersection = Area(
            left=max(x.area.left for x in layers),
            top=min(x.area.top for x in layers),
            right=min(x.area.right for x in layers),
            bottom=max(x.area.bottom for x in layers)
        )
        if (intersection.left >= intersection.right) or (intersection.bottom >= intersection.top):
            raise ValueError('No intersection possible')
        return intersection

    @classmethod
    def layer_from_file(cls, filename: str):
        dataset = gdal.Open(filename, gdal.GA_ReadOnly)
        if dataset is None:
            raise FileNotFoundError(filename)
        return cls(dataset)

    def __init__(self, dataset: gdal.Dataset):
        if not dataset:
            raise ValueError("None is not a valid dataset")
        self._dataset = dataset
        self._transform = dataset.GetGeoTransform()
        self._rasterXSize = dataset.RasterXSize
        self._rasterYSize = dataset.RasterYSize
        self._intersection = None

        # Global position of the layer
        self.area = Area(
            left=self._transform[0],
            top=self._transform[3],
            right=self._transform[0] + (self._rasterXSize * self._transform[1]),
            bottom=self._transform[3] + (self._rasterYSize * self._transform[5]),
        )

        # default window to full layer
        self.window = Window(
            xoff=0,
            yoff=0,
            xsize=dataset.RasterXSize,
            ysize=dataset.RasterYSize,
        )

    @property
    def geo_transform(self) -> Tuple[float]:
        if self._intersection:
            return (
                self._intersection.left, self._transform[1],
                0.0, self._intersection.top, 0.0, self._transform[5]
            )
        else:
            return self._transform

    @property
    def pixel_scale(self) -> PixelScale:
        return PixelScale(self._transform[1], self._transform[5])

    @property
    def projection(self) -> str:
        return self._dataset.GetProjection()

    def check_pixel_scale(self, scale) -> bool:
        return self.pixel_scale == scale

    def set_window_for_intersection(self, intersection: Area) -> None:
        new_window = Window(
            xoff=int((intersection.left - self.area.left) / self._transform[1]),
            yoff=int((self.area.top - intersection.top) / (self._transform[5] * -1.0)),
            xsize=int((intersection.right - intersection.left) / self._transform[1]),
            ysize=int((intersection.top - intersection.bottom) / (self._transform[5] * -1.0)),
        )
        if (new_window.xoff < 0) or (new_window.yoff < 0):
            raise ValueError('Window has negative offset')
        if ((new_window.xoff + new_window.xsize) > self._rasterXSize) or ((new_window.yoff + new_window.ysize) > self._rasterYSize):
            raise ValueError('Window is bigger than dataset')
        self.window = new_window
        self._intersection = intersection

    def ReadAsArray(self, x, y, xsize, ysize) -> Any:
        return self._dataset.GetRasterBand(1).ReadAsArray(
            self.window.xoff + x,
            self.window.yoff + y,
            xsize, ysize
        )


class VectorRangeLayer(Layer):
    def __init__(self, range_vectors: str, where_filter: str, scale: PixelScale, projection: str):
        vectors = ogr.Open(range_vectors)
        if vectors is None:
            raise FileNotFoundError(range_vectors)
        range_layer = vectors.GetLayer()
        range_layer.SetAttributeFilter(where_filter)

        # work out region for mask
        envelopes = []
        range_layer.ResetReading()
        feature = range_layer.GetNextFeature()
        while feature:
            envelopes.append(feature.GetGeometryRef().GetEnvelope())
            feature = range_layer.GetNextFeature()
        if len(envelopes) == 0:
            raise ValueError(f'No geometry found for {where_filter}')

        # Get the area, but scale it to the pixel resolution that we're using. Note that
        # the pixel scale GDAL uses can have -ve values, but those will mess up the
        # ceil/floor math, so we use absolute versions when trying to round.
        abs_xstep, abs_ystep = abs(scale.xstep), abs(scale.ystep)
        area = Area(
            left=floor(min(x[0] for x in envelopes) / abs_xstep) * abs_xstep,
            top=ceil(max(x[3] for x in envelopes) / abs_ystep) * abs_ystep,
            right=ceil(max(x[1] for x in envelopes) / abs_xstep) * abs_xstep,
            bottom=floor(min(x[2] for x in envelopes) / abs_ystep) * abs_ystep,
        )

        # create new dataset for just that area
        dataset = gdal.GetDriverByName('mem').Create(
            'mem',
            int((area.right - area.left) / abs_xstep),
            int((area.top - area.bottom) / abs_ystep),
            1,
            gdal.GDT_Byte,
            []
        )
        if not dataset:
            raise Exception(f'Failed to create memory mask')

        dataset.SetProjection(projection)
        dataset.SetGeoTransform([area.left, scale.xstep, 0.0, area.top, 0.0, scale.ystep])
        gdal.RasterizeLayer(dataset, [1], range_layer, burn_values=[1], options=["ALL_TOUCHED=TRUE"])

        super().__init__(dataset)


class UniformAreaLayer(Layer):

    @staticmethod
    def generateNarrowAreaProjection(source_filename: str, target_filename: str) -> None:
        source = gdal.Open(source_filename, gdal.GA_ReadOnly)
        if source is None:
            raise FileNotFoundError(source_filename)
        # if not UniformAreaLayer.isUniformAreaProjection(source):
        #     raise ValueError("Data in area pixel map is not uniform across rows")
        source_band = source.GetRasterBand(1)
        target = gdal.GetDriverByName('GTiff').Create(
            target_filename,
            1,
            source.RasterYSize,
            1,
            source_band.DataType,
            ['COMPRESS=LZW']
        )
        target.SetProjection(source.GetProjection())
        target.SetGeoTransform(source.GetGeoTransform())
        data = source_band.ReadAsArray(0, 0, 1, source.RasterYSize)
        target_band = target.GetRasterBand(1)
        target_band.WriteArray(data, 0, 0)


    @staticmethod
    def isUniformAreaProjection(dataset) -> bool:
        "Check that the dataset conforms to the assumption that all rows contain the same value. Likely to be slow."
        band = dataset.GetRasterBand(1)
        for y in range(dataset.RasterYSize):
            row = band.ReadAsArray(0, y, dataset.RasterXSize, 1)
            if not numpy.all(numpy.isclose(row, row[0])):
                return False
        return True

    def __init__(self, dataset):
        if dataset.RasterXSize > 1:
            raise ValueError("Expected a shrunk dataset")
        self.databand = dataset.GetRasterBand(1).ReadAsArray(0, 0, 1, dataset.RasterYSize)

        super().__init__(dataset)

        transform = dataset.GetGeoTransform()
        self.window = Window(
            xoff=0,
            yoff=0,
            xsize=360 / transform[1],
            ysize=dataset.RasterYSize,
        )
        self._rasterXSize = self.window.xsize
        self.area = Area(-180, self.area.top, 180, self.area.bottom)

    def ReadAsArray(self, x, y, xsize, ysize) -> Any:
        offset = self.window.yoff + y
        subset = self.databand[offset:offset + ysize][0]
        return subset


class NullLayer:
    def __init__(self):
        self.area = Area(
            left = -180.0,
            top = 90.0,
            right = 180.0,
            bottom = -90.0
        )

    def pixel_scale(self) -> PixelScale:
        return None

    def check_pixel_scale(self, scale: PixelScale) -> bool:
        return True

    def set_window_for_intersection(self, intersection: Area) -> None:
        pass

    def ReadAsArray(self, x, y, xsize, ysize) -> Any:
        return 1.0
