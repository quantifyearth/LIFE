
import numpy as np
import pytest
import yirgacheffe as yg
from yirgacheffe.layers import RasterLayer
from yirgacheffe.operators import DataType
from yirgacheffe.window import Area, PixelScale

from prepare_layers.make_food_current_map import TileInfo, process_tile

@pytest.mark.parametrize("initial,crop_diff,pasture_diff,expected", [
    (42, float("nan"), float("nan"), 42),

    # Other habitat replacement
    (42, 1.0, float("nan"), 1401),
    (42, float("nan"), 1.0, 1402),
    (42, 0.0, float("nan"), 42),
    (42, float("nan"), 0.0, 42),
    (42, -1.0, float("nan"), 42),
    (42, float("nan"), -1.0, 42),

    # Crop replacement
    (1401, 1.0, float("nan"), 1401),
    (1401, float("nan"), 1.0, 1401),
    (1401, 0.0, float("nan"), 1401),
    (1401, float("nan"), 0.0, 1401),
    (1401, -1.0, float("nan"), 31),
    (1401, -1.0, 1.0, 1402),
    (1401, float("nan"), -1.0, 1401),

    # Pasture replacement
    (1402, 1.0, float("nan"), 1402),
    (1402, 1.0, float("nan"), 1402),
    (1402, float("nan"), 0.0, 1402),
    (1402, 0.0, float("nan"), 1402),
    (1402, float("nan"), -1.0, 31),
    (1402, 1.0, -1.0, 1401),
    (1402, -1.0, float("nan"), 1402),

    # Don't touch urban
    (1405, 1.0, float("nan"), 1405),
    (1405, float("nan"), 1.0, 1405),
    (1405, 0.0, float("nan"), 1405),
    (1405, float("nan"), 0.0, 1405),
    (1405, -1.0, float("nan"), 1405),
    (1405, -1.0, 1.0, 1405),
    (1405, float("nan"), -1.0, 1405),

])
def test_process_tile_all(initial, crop_diff, pasture_diff, expected) -> None:
    with RasterLayer.empty_raster_layer(Area(-180, 90, 180, -90), PixelScale(1.0, -1.0), datatype=DataType.Int16) as current:
        yg.constant(initial).save(current)
        with RasterLayer.empty_raster_layer(Area(-180, 90, 180, -90), PixelScale(1.0, -1.0), datatype=DataType.Int16) as pnv:
            yg.constant(31).save(pnv)

            test_tile = TileInfo(
                x_position=0,
                y_position=0,
                width=10,
                height=12,
                crop_diff=crop_diff,
                pasture_diff=pasture_diff,
            )

            result = process_tile(current, pnv, test_tile)
            expected = np.full((12, 10), expected, dtype=np.int16)
            assert (result == expected).all()

@pytest.mark.parametrize("initial,crop_diff,pasture_diff,expected_crop_count,expected_pasture_count,expected_pnv_count", [
    (42, float("nan"), float("nan"), 0, 0, 0),
    (42, 0.5, float("nan"), 50, 0, 0),
    (42, float("nan"), 0.5, 0, 50, 0),
    (42, -0.5, float("nan"), 0, 0, 0),
    (42, float("nan"), -0.5, 0, 0, 0),

    (1401, float("nan"), float("nan"), 100, 0, 0),
    (1401, -0.5, float("nan"), 50, 0, 50),
    (1401, float("nan"), -0.5, 100, 0, 0),

    (1402, float("nan"), float("nan"), 0, 100, 0),
    (1402, float("nan"), -0.5, 0, 50, 50),
    (1402, -0.5, float("nan"), 0, 100, 0),
])
def test_partial_replacement(initial, crop_diff, pasture_diff, expected_crop_count, expected_pasture_count, expected_pnv_count) -> None:
    with RasterLayer.empty_raster_layer(Area(-180, 90, 180, -90), PixelScale(1.0, -1.0), datatype=DataType.Int16) as current:
        yg.constant(initial).save(current)
        with RasterLayer.empty_raster_layer(Area(-180, 90, 180, -90), PixelScale(1.0, -1.0), datatype=DataType.Int16) as pnv:
            yg.constant(31).save(pnv)

            test_tile = TileInfo(
                x_position=0,
                y_position=0,
                width=10,
                height=10,
                crop_diff=crop_diff,
                pasture_diff=pasture_diff,
            )

            result = process_tile(current, pnv, test_tile)
            crop_count = (result == 1401).sum()
            assert crop_count == expected_crop_count
            pasture_count = (result == 1402).sum()
            assert pasture_count == expected_pasture_count
            pnv_count = (result == 31).sum()
            assert pnv_count == expected_pnv_count

@pytest.mark.parametrize("initial,crop_diff,pasture_diff,expected_crop_count,expected_pasture_count,expected_pnv_count", [
    (42, float("nan"), float("nan"), 0, 0, 0),
    (42, 0.5, float("nan"), 50, 0, 0),
    (42, float("nan"), 0.5, 0, 50, 0),
    (42, -0.5, float("nan"), 0, 0, 0),
    (42, float("nan"), -0.5, 0, 0, 0),

    (1401, float("nan"), float("nan"), 50, 0, 0),
    (1401, 1.0, float("nan"), 100, 0, 0),
    (1401, 0.5, float("nan"), 100, 0, 0),
    (1401, 0.1, float("nan"), 60, 0, 0),
    (1401, -0.1, float("nan"), 40, 0, 10),
    (1401, -0.5, float("nan"), 0, 0, 50),
    (1401, -1.0, float("nan"), 0, 0, 50),
    (1401, float("nan"), 1.0, 50, 50, 0),

    (1405, float("nan"), float("nan"), 0, 0, 0),
    (1405, 1.0, float("nan"), 50, 0, 0),
    (1405, 0.5, float("nan"), 50, 0, 0),
    (1405, 0.1, float("nan"), 10, 0, 0),
    (1405, -0.1, float("nan"), 0, 0, 0),
    (1405, -0.5, float("nan"), 0, 0, 0),
    (1405, -1.0, float("nan"), 0, 0, 0),
])
def test_partial_initial_tile(initial, crop_diff, pasture_diff, expected_crop_count, expected_pasture_count, expected_pnv_count) -> None:
    with RasterLayer.empty_raster_layer(Area(-180, 90, 180, -90), PixelScale(1.0, -1.0), datatype=DataType.Int16) as current:

        # Cheating as Yirgacheffe doesn't have a neat way to provide numpy data straight to a layer
        band = current._dataset.GetRasterBand(1)
        vals = np.array([[initial, 22], [22, initial]])
        all_vals = np.tile(vals, (90, 180))
        band.WriteArray(all_vals, 0, 0)

        with RasterLayer.empty_raster_layer(Area(-180, 90, 180, -90), PixelScale(1.0, -1.0), datatype=DataType.Int16) as pnv:
            yg.constant(31).save(pnv)

            test_tile = TileInfo(
                x_position=0,
                y_position=0,
                width=10,
                height=10,
                crop_diff=crop_diff,
                pasture_diff=pasture_diff,
            )

            result = process_tile(current, pnv, test_tile)
            crop_count = (result == 1401).sum()
            assert crop_count == expected_crop_count
            pasture_count = (result == 1402).sum()
            assert pasture_count == expected_pasture_count
            pnv_count = (result == 31).sum()
            assert pnv_count == expected_pnv_count
