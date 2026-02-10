
import numpy as np
import pytest
import yirgacheffe as yg
from yirgacheffe.layers import RasterLayer
from yirgacheffe.operators import DataType
from yirgacheffe.window import Area, PixelScale

from prepare_layers.make_food_current_map import TileInfo, process_tile, balance_crop_and_pasture_differences, \
    CROP_CODE, PASTURE_CODE, remove_land_cover, add_land_cover

@pytest.mark.parametrize("initial_crop_diff,initial_pasture_diff,expected_crop_diff,expected_pasture_diff", [
    (0.0, 0.0, 0.0, 0.0),
    (0.8, 0.0, 0.8, 0.0),
    (-0.8, 0.0, -0.8, 0.0),
    (0.0, 0.8, 0.0, 0.8),
    (0.0, -0.8, 0.0, -0.8),
    (0.4, 0.2, 0.4, 0.2),
    (-0.4, -0.2, -0.4, -0.2),
])
def test_balance_not_needed(
    initial_crop_diff: float,
    initial_pasture_diff: float,
    expected_crop_diff: float,
    expected_pasture_diff: float,
) -> None:
    result_crop_diff, result_pasture_diff = balance_crop_and_pasture_differences(
        initial_crop_diff,
        initial_pasture_diff,
        dict(),
    )
    assert expected_crop_diff == result_crop_diff
    assert expected_pasture_diff == result_pasture_diff


def test_one_sided_balance() -> None:
    lcc_data_map = {
        CROP_CODE: np.zeros((10, 10)),
        PASTURE_CODE: np.zeros((10, 10)),
    }
    result_crop_diff, result_pasture_diff = balance_crop_and_pasture_differences(
        0.5,
        -0.1,
        lcc_data_map,
    )
    # assert expected_crop_diff == result_crop_diff
    # assert expected_pasture_diff == result_pasture_diff
    # assert (lcc_data_map[CROP_CODE] == crop_cell_value).all()
    # assert (lcc_data_map[PASTURE_CODE] == pasture_cell_value).all()


@pytest.mark.parametrize(
    "initial_crop_diff,initial_pasture_diff,expected_crop_diff,expected_pasture_diff,crop_cell_value,pasture_cell_value",
    [
        (0.25, -0.5, 0.0, -0.25, 0.25, 0.75),
    ]
)
def test_simple_balance_transfer(
    initial_crop_diff: float,
    initial_pasture_diff: float,
    expected_crop_diff: float,
    expected_pasture_diff: float,
    crop_cell_value: float,
    pasture_cell_value: float,
) -> None:
    # 0% crop, 100% pasture
    lcc_data_map = {
        CROP_CODE: np.zeros((10, 10)),
        PASTURE_CODE: np.ones((10, 10)),
    }
    result_crop_diff, result_pasture_diff = balance_crop_and_pasture_differences(
        initial_crop_diff,
        initial_pasture_diff,
        lcc_data_map,
    )
    assert expected_crop_diff == result_crop_diff
    assert expected_pasture_diff == result_pasture_diff
    assert (lcc_data_map[CROP_CODE] == crop_cell_value).all()
    assert (lcc_data_map[PASTURE_CODE] == pasture_cell_value).all()


@pytest.mark.parametrize(
    "initial_crop_diff,initial_pasture_diff,expected_crop_diff,expected_pasture_diff,crop_cell_value,pasture_cell_value",
    [
        (0.25, -0.5, 0.0, -0.25, 0.5, 0.5),
        (0.5, -0.25, 0.25, 0.0, 0.5, 0.5),
    ]
)
def test_simple_half_balance_transfer(
    initial_crop_diff: float,
    initial_pasture_diff: float,
    expected_crop_diff: float,
    expected_pasture_diff: float,
    crop_cell_value: float,
    pasture_cell_value: float,
) -> None:
    # 0% crop, 50% pasture
    lcc_data_map = {
        CROP_CODE: np.zeros((10, 10)),
        PASTURE_CODE: np.array([[i % 2] * 10 for i in range(10)]).astype(float),
    }
    result_crop_diff, result_pasture_diff = balance_crop_and_pasture_differences(
        initial_crop_diff,
        initial_pasture_diff,
        lcc_data_map,
    )
    assert expected_crop_diff == result_crop_diff
    assert expected_pasture_diff == result_pasture_diff

    expected_crop_map = np.array([[i % 2] * 10 for i in range(10)]).astype(float) * crop_cell_value
    expected_pasture_map = np.array([[i % 2] * 10 for i in range(10)]).astype(float) * pasture_cell_value
    assert (expected_crop_map == lcc_data_map[CROP_CODE]).all()
    assert (expected_pasture_map == lcc_data_map[PASTURE_CODE]).all()


@pytest.mark.parametrize("crop_diff,expected_crop_cell,expected_other_cell", [
    (-0.5, 0.5, 0.5),
])
def test_remove_land_simple(
    crop_diff: float,
    expected_crop_cell: float,
    expected_other_cell: float,
) -> None:
    # 100% crop, 0% other 1
    lcc_data_map = {
        1: np.zeros((10, 10)),
        CROP_CODE: np.ones((10, 10)),
    }
    pnv_data = np.full((10, 10), 1)

    remove_land_cover(
        CROP_CODE,
        crop_diff,
        pnv_data,
        lcc_data_map,
    )

    expected_crop_map = np.full((10, 10), expected_crop_cell)
    expected_other_map = np.full((10, 10), expected_other_cell)
    assert (expected_crop_map == lcc_data_map[CROP_CODE]).all()
    assert (expected_other_map == lcc_data_map[1]).all()


@pytest.mark.parametrize("crop_diff,pnv_value,expected_crop_cell,expected_other_1_cell,expected_other_2_cell", [
    (-0.5, 1, 0.0, 1.0, 0.0),
    (-0.75, 1, 0.0, 1.0, 0.0), # too much
    (-0.25, 1, 0.5, 0.5, 0.0),
    (-0.5, 2, 0.0, 0.0, 1.0),
    (-0.25, 2, 0.5, 0.0, 0.5),
])
def test_remove_land_less_simple(
    crop_diff: float,
    pnv_value: int,
    expected_crop_cell: float,
    expected_other_1_cell: float,
    expected_other_2_cell: float,
) -> None:
    # 50% crop, 50% other 2, 0% other 1
    lcc_data_map = {
        1: np.zeros((10, 10)),
        2: np.array([[(i + 1) % 2] * 10 for i in range(10)]).astype(float),
        CROP_CODE: np.array([[i % 2] * 10 for i in range(10)]).astype(float),
    }
    pnv_data = np.full((10, 10), pnv_value)

    remove_land_cover(
        CROP_CODE,
        crop_diff,
        pnv_data,
        lcc_data_map,
    )

    expected_crop_map = np.array([[i % 2] * 10 for i in range(10)]).astype(float) * expected_crop_cell
    assert (expected_crop_map == lcc_data_map[CROP_CODE]).all()
    expected_other_1_map = np.array([[i % 2] * 10 for i in range(10)]).astype(float) * expected_other_1_cell
    assert (expected_other_1_map == lcc_data_map[1]).all()
    expected_other_2_map = np.array([[(i + 1) % 2] * 10 for i in range(10)]).astype(float) + \
        (np.array([[i % 2] * 10 for i in range(10)]).astype(float) * expected_other_2_cell)
    assert (expected_other_2_map == lcc_data_map[2]).all()

@pytest.mark.parametrize("crop_diff,expected_crop_cell,expected_other_cell", [
    (0.5, 0.5, 0.5),
    (1.0, 1.0, 0.0),
])
def test_add_land_simple(
    crop_diff: float,
    expected_crop_cell: float,
    expected_other_cell: float,
) -> None:
    # 100% crop, 0% other 1
    lcc_data_map = {
        1: np.ones((10, 10)),
        CROP_CODE: np.zeros((10, 10)),
    }
    pnv_data = np.full((10, 10), 1)

    add_land_cover(
        CROP_CODE,
        crop_diff,
        lcc_data_map,
    )

    expected_crop_map = np.full((10, 10), expected_crop_cell)
    expected_other_map = np.full((10, 10), expected_other_cell)
    assert (expected_crop_map == lcc_data_map[CROP_CODE]).all()
    assert (expected_other_map == lcc_data_map[1]).all()

@pytest.mark.parametrize("crop_diff,expected_crop_cell,expected_other_cell", [
    (0.25, 0.5, 0.5),
    (0.5, 1.0, 0.0),
])
def test_add_land_avoid_excluded(
    crop_diff: float,
    expected_crop_cell: float,
    expected_other_cell: float,
) -> None:
    # 100% crop, 0% other 1
    lcc_data_map = {
        1: np.array([[(i + 1) % 2] * 10 for i in range(10)]).astype(float),
        PASTURE_CODE: np.array([[i % 2] * 10 for i in range(10)]).astype(float),
        CROP_CODE: np.zeros((10, 10)),
    }
    pnv_data = np.full((10, 10), 1)

    add_land_cover(
        CROP_CODE,
        crop_diff,
        lcc_data_map,
    )

    expected_crop_map = np.array([[(i + 1) % 2] * 10 for i in range(10)]).astype(float) * expected_crop_cell
    expected_pasture_map = np.array([[i % 2] * 10 for i in range(10)]).astype(float) # unchanged
    expected_other_map = np.array([[(i + 1) % 2] * 10 for i in range(10)]).astype(float) * expected_other_cell
    assert (expected_crop_map == lcc_data_map[CROP_CODE]).all()
    assert (expected_pasture_map == lcc_data_map[PASTURE_CODE]).all()
    assert (expected_other_map == lcc_data_map[1]).all()
