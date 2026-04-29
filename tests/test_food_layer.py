import math

import numpy as np
import pytest
from pytest import param as P
import yirgacheffe as yg

from prepare_layers.make_food_current_map import balance_crop_and_pasture_differences, \
    CROP_CODE, PASTURE_CODE, remove_land_cover, add_land_cover, TileInfo, process_tile, PRESERVE_CODES

@pytest.mark.parametrize(
    [
        "initial_crop_diff",
        "initial_pasture_diff",
        "expected_crop_diff",
        "expected_pasture_diff"
    ],
    [
        (0.0, 0.0, 0.0, 0.0),
        (0.8, 0.0, 0.8, 0.0),
        (-0.8, 0.0, -0.8, 0.0),
        (0.0, 0.8, 0.0, 0.8),
        (0.0, -0.8, 0.0, -0.8),
        (0.4, 0.2, 0.4, 0.2),
        (-0.4, -0.2, -0.4, -0.2),
    ]
)
def test_balance_not_needed(
    initial_crop_diff: float,
    initial_pasture_diff: float,
    expected_crop_diff: float,
    expected_pasture_diff: float,
) -> None:
    result_crop_diff, result_pasture_diff = balance_crop_and_pasture_differences(
        initial_crop_diff,
        initial_pasture_diff,
        {},
    )
    assert expected_crop_diff == result_crop_diff
    assert expected_pasture_diff == result_pasture_diff


def test_brokend_balance() -> None:
    # Testing that we spot if we've said to remove land where there isn't any
    lcc_data_map = {
        CROP_CODE: np.zeros((10, 10)),
        PASTURE_CODE: np.zeros((10, 10)),
    }
    with pytest.raises(ValueError):
        _ = balance_crop_and_pasture_differences(
            0.5,
            -0.1,
            lcc_data_map,
        )


@pytest.mark.parametrize(
    [
        "initial_crop_diff",
        "initial_pasture_diff",
        "expected_crop_diff",
        "expected_pasture_diff",
        "crop_cell_value",
        "pasture_cell_value"
    ],
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
    [
        "initial_crop_diff",
        "initial_pasture_diff",
        "expected_crop_diff",
        "expected_pasture_diff",
        "crop_cell_value",
        "pasture_cell_value"
    ],
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


@pytest.mark.parametrize(
    [
        "crop_diff",
        "expected_crop_cell",
        "expected_other_cell",
    ],
    [
        (-0.5, 0.5, 0.5),
    ]
)
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


@pytest.mark.parametrize(
    [
        "crop_diff",
        "pnv_value",
        "expected_crop_cell",
        "expected_other_1_cell",
        "expected_other_2_cell",
    ],
    [
        (-0.5, 1, 0.0, 1.0, 0.0),
        (-0.75, 1, 0.0, 1.0, 0.0), # too much
        (-0.25, 1, 0.5, 0.5, 0.0),
        (-0.5, 2, 0.0, 0.0, 1.0),
        (-0.25, 2, 0.5, 0.0, 0.5),
    ]
)
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

    add_land_cover(
        np.ones((10, 10), dtype=bool),
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

    add_land_cover(
        np.array([[(i + 1) % 2] * 10 for i in range(10)]).astype(bool),
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


@pytest.mark.parametrize(["crop_diff", "pasture_diff", "expected_totals"], [
    P(0.25, 0.25, {1: 25, CROP_CODE: 25, PASTURE_CODE: 25, 4: 25}, id="no change"),
    P(0.0, 0.0, {1: 75, CROP_CODE: 0, PASTURE_CODE: 0, 4: 25}, id="remove both"),
    P(0.25, 0.0, {1: 50, CROP_CODE: 25, PASTURE_CODE: 0, 4: 25}, id="remove pasture, leave crop"),
    P(0.0, 0.25, {1: 50, CROP_CODE: 0, PASTURE_CODE: 25, 4: 25}, id="remove crop, leave pasture"),
    P(0.5, 0.25, {1: 12.5, CROP_CODE: 50, PASTURE_CODE: 25, 4: 12.5}, id="add crop, leave pasture"),
    P(0.25, 0.5, {1: 12.5, CROP_CODE: 25, PASTURE_CODE: 50, 4: 12.5}, id="leave crop, add pasture"),
    P(0.5, 0.2, {1: 15, CROP_CODE: 50, PASTURE_CODE: 20, 4: 15}, id="add crop, remove pasture, more add than remove"),
    P(0.2, 0.5, {1: 15, CROP_CODE: 20, PASTURE_CODE: 50, 4: 15}, id="remove crop, add pasture, more add than remove"),
    P(0.3, 0.1, {1: 35, CROP_CODE: 30, PASTURE_CODE: 10, 4: 25}, id="add crop, remove pasture, more remove than add"),
    P(0.1, 0.3, {1: 35, CROP_CODE: 10, PASTURE_CODE: 30, 4: 25}, id="remove crop, add pasture, more remove than add"),
    P(0.3, 0.3, {1: 20, CROP_CODE: 30, PASTURE_CODE: 30, 4: 20}, id="all both, but not total"),
    P(0.5, 0.5, {1: 0, CROP_CODE: 50, PASTURE_CODE: 50, 4: 0}, id="all both"),
    P(0.0, 0.5, {1: 25, CROP_CODE: 0, PASTURE_CODE: 50, 4: 25}, id="replace crop with pasture"),
    P(0.5, 0.0, {1: 25, CROP_CODE: 50, PASTURE_CODE: 0, 4: 25}, id="replace pasture with crop"),
    P(0.0, 1.0, {1: 0, CROP_CODE: 0, PASTURE_CODE: 100, 4: 0}, id="All pasture, all the time"),
    P(1.0, 0.0, {1: 0, CROP_CODE: 100, PASTURE_CODE: 0, 4: 0}, id="All crop, all the time"),
])
def test_process_tile(crop_diff: float, pasture_diff: float, expected_totals: dict[int, float]) -> None:
    # One quarter neither, one quarter crop, one quarter pasture, one quarter other neither
    data = np.ones((5, 5))
    raw_lcc_data_map = {
        1:            np.pad(data, ((0, 5), (0, 5))),
        CROP_CODE:    np.pad(data, ((0, 5), (5, 0))),
        PASTURE_CODE: np.pad(data, ((5, 0), (0, 5))),
        4:            np.pad(data, ((5, 0), (5, 0))),
    }
    projection = yg.MapProjection("epsg:4326", 1.0, -1.0)
    lcc_maps = {lcc: yg.from_array(x, (0, 0), projection) for lcc, x in raw_lcc_data_map.items()}

    # The pnv is just class 1, so that is always the category that will increase, class 4 will only
    # only go below its initial value or be the same
    pnv_data = np.ones((10, 10)) # All class 1
    pnv_map = yg.from_array(pnv_data, (0, 0), projection)

    tile = TileInfo(
        x_position=0,
        y_position=0,
        width=10,
        height=10,
        crop_target=crop_diff,
        pasture_target=pasture_diff,
    )

    updated_maps = process_tile(lcc_maps, pnv_map, tile)

    for key, expected_value in expected_totals.items():
        layer = updated_maps[key]
        layer_total = np.sum(layer)
        assert layer_total >= 0
        assert math.isclose(layer_total, expected_value, rel_tol=0.000001), f"Failed for {key}"


@pytest.mark.parametrize(["crop_diff", "pasture_diff", "expected_totals"], [
    P(0.25, 0.25, {1: 25, CROP_CODE: 25, PASTURE_CODE: 25}, id="no change"),
    P(0.0, 0.0, {1: 75, CROP_CODE: 0, PASTURE_CODE: 0}, id="remove both"),
    P(0.25, 0.0, {1: 50, CROP_CODE: 25, PASTURE_CODE: 0}, id="remove pasture, leave crop"),
    P(0.0, 0.25, {1: 50, CROP_CODE: 0, PASTURE_CODE: 25}, id="remove crop, leave pasture"),
    P(0.5, 0.25, {1: 0, CROP_CODE: 50, PASTURE_CODE: 25}, id="add crop, leave pasture"),
    P(0.25, 0.5, {1: 0, CROP_CODE: 25, PASTURE_CODE: 50}, id="leave crop, add pasture"),
    P(0.5, 0.2, {1: 5, CROP_CODE: 50, PASTURE_CODE: 20}, id="add crop, remove pasture, more add than remove"),
    P(0.2, 0.5, {1: 5, CROP_CODE: 20, PASTURE_CODE: 50}, id="remove crop, add pasture, more add than remove"),
    P(0.3, 0.1, {1: 35, CROP_CODE: 30, PASTURE_CODE: 10}, id="add crop, remove pasture, more remove than add"),
    P(0.1, 0.3, {1: 35, CROP_CODE: 10, PASTURE_CODE: 30}, id="remove crop, add pasture, more remove than add"),
    P(0.3, 0.3, {1: 15, CROP_CODE: 30, PASTURE_CODE: 30}, id="all both, but not total"),
    P(0.5, 0.5, {1: 0, CROP_CODE: 37.5, PASTURE_CODE: 37.5}, id="all both (unobtainable)"),
    P(0.0, 0.5, {1: 25, CROP_CODE: 0, PASTURE_CODE: 50}, id="replace crop with pasture"),
    P(0.5, 0.0, {1: 25, CROP_CODE: 50, PASTURE_CODE: 0}, id="replace pasture with crop"),
    P(0.0, 1.0, {1: 0, CROP_CODE: 0, PASTURE_CODE: 75}, id="All pasture, all the time"),
    P(1.0, 0.0, {1: 0, CROP_CODE: 75, PASTURE_CODE: 0}, id="All crop, all the time"),
    P(0.6, 0.2, {1: 0, CROP_CODE: 55, PASTURE_CODE: 20}, id="Less pasture, but too much crop demand"),
    P(0.2, 0.6, {1: 0, CROP_CODE: 20, PASTURE_CODE: 55}, id="Less crop, but too much pasture demand"),
])
def test_process_tile_with_preserve_codes(
    crop_diff: float,
    pasture_diff: float,
    expected_totals: dict[int, float]
) -> None:
    # One quarter neither, one quarter crop, one quarter pasture, one quarter preserved
    data = np.ones((5, 5))
    raw_lcc_data_map = {
        1:            np.pad(data, ((0, 5), (0, 5))),
        CROP_CODE:    np.pad(data, ((0, 5), (5, 0))),
        PASTURE_CODE: np.pad(data, ((5, 0), (0, 5))),
        PRESERVE_CODES[0]: np.pad(data, ((5, 0), (5, 0))),
    }
    projection = yg.MapProjection("epsg:4326", 1.0, -1.0)
    lcc_maps = {lcc: yg.from_array(x, (0, 0), projection) for lcc, x in raw_lcc_data_map.items()}

    # The pnv is just class 1, so that is always the category that will increase
    pnv_data = np.ones((10, 10)) # All class 1
    pnv_map = yg.from_array(pnv_data, (0, 0), projection)

    tile = TileInfo(
        x_position=0,
        y_position=0,
        width=10,
        height=10,
        crop_target=crop_diff,
        pasture_target=pasture_diff,
    )

    updated_maps = process_tile(lcc_maps, pnv_map, tile)

    # This quarter of the tile should never be modified
    expected_totals[PRESERVE_CODES[0]] = 25

    for key, expected_value in expected_totals.items():
        layer = updated_maps[key]
        layer_total = np.sum(layer)
        assert layer_total >= 0
        assert math.isclose(layer_total, expected_value, rel_tol=0.000001), f"Failed for {key}"


def test_ensure_no_negative_values() -> None:
    # This test case is something that was seen in real world use: although the math
    # generally should prevent us going negative, it does happen due to rounding. This
    # test case tries to force a situation where we'd get a negative value, and we
    # want to ensure it's clipped to zero instead.
    shape = (4,)
    eligible_mask = np.array([True, True, False, False])

    crop = np.zeros(shape)
    other = np.array([0.05, 0.05, 0.8, 0.8])

    lcc_data_map = {
        CROP_CODE: crop,
        1: other,
    }

    add_land_cover(eligible_mask, CROP_CODE, 0.1, lcc_data_map)

    for lcc, lcc_data in lcc_data_map.items():
        print(lcc_data)
        assert (lcc_data >= 0).all()
