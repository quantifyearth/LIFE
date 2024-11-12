import pytest
import postgis

from prepare_species.extract_species_psql import process_habitats, process_geometries

@pytest.mark.parametrize("label", [
    "resident",
    "Resident",
    "unknown",
    "Seasonal Occurrence Unknown",
    None
])
def test_simple_resident_species_habitat_filter(label):
    habitat_data = [
        (label, "Yes", "4.1|4.2", "Terrestrial"),
        (label, "No", "4.3", "Terrestrial"),
    ]
    res = process_habitats(habitat_data)

    assert list(res.keys()) == [1]
    assert res[1] == set(["4.1", "4.2", "4.3"])

@pytest.mark.parametrize("breeding_label,non_breeding_label", [
    ("breeding", "non-breeding"),
    ("Breeding Season", "non-breeding"),
    ("breeding", "Non-Breeding Season"),
    ("Breeding Season", "Non-Breeding Season"),
])
def test_simple_migratory_species_habitat_filter(breeding_label, non_breeding_label):
    habitat_data = [
        (breeding_label, "Yes", "4.1|4.2", "Terrestrial"),
        (non_breeding_label, "No", "4.3", "Terrestrial"),
    ]
    res = process_habitats(habitat_data)

    assert list(res.keys()) == [2, 3]
    assert res[2] == set(["4.1", "4.2"])
    assert res[3] == set(["4.3"])

def test_reject_if_marine_in_system():
    habitat_data = [
        ("resident", "Yes", "4.1|4.2", "Terrestrial"),
        ("resident", "No", "4.3", "Terrestrial|Marine"),
    ]
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data)

def test_reject_if_caves_in_major_habitat():
    habitat_data = [
        ("resident", "Yes", "4.1|7.2", "Terrestrial"),
        ("resident", "No", "4.3", "Terrestrial"),
    ]
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data)

def test_do_not_reject_if_caves_in_minor_habitat():
    habitat_data = [
        ("resident", "Yes", "4.1|4.2", "Terrestrial"),
        ("resident", "No", "7.3", "Terrestrial"),
    ]
    res = process_habitats(habitat_data)

    # Just resident
    assert list(res.keys()) == [1]
    assert res[1] == set(["4.1", "4.2", "7.3"])

@pytest.mark.parametrize("label", [
    "passage",
    "Passage",
])
def test_passage_habitat_ignored(label):
    habitat_data = [
        ("resident", "Yes", "4.1|4.2", "Terrestrial"),
        (label, "No", "4.3", "Terrestrial"),
    ]
    res = process_habitats(habitat_data)

    # Just resident
    assert list(res.keys()) == [1]
    assert res[1] == set(["4.1", "4.2"])

def test_fail_no_habitats_before_filter():
    habitat_data = []
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data)

def test_fail_no_habitats_after_filter():
    habitat_data = [
        ("passage", "Yes", "4.1|7.2", "Terrestrial"),
    ]
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data)

def test_fail_if_unrecognised_season_for_habitat():
    habitat_data = [
        ("resident", "Yes", "4.1|4.2", "Terrestrial"),
        ("zarquon", "Yes", "4.3", "Terrestrial"),
    ]
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data)

def test_empty_geometry_list():
    geoemetries_data = []
    with pytest.raises(ValueError):
        _ = process_geometries(geoemetries_data)


@pytest.mark.parametrize("label", [
    1,
    5
])
def test_simple_resident_species_geometry_filter(label):
    habitat_data = [
        (label, postgis.Geometry.from_ewkb("000000000140000000000000004010000000000000")),
    ]
    res = process_geometries(habitat_data)

    assert list(res.keys()) == [1]

def test_simple_migratory_species_geometry_filter():
    habitat_data = [
        (2, postgis.Geometry.from_ewkb("000000000140000000000000004010000000000000")),
        (3, postgis.Geometry.from_ewkb("000000000140000000000000004010000000000000")),
    ]
    res = process_geometries(habitat_data)

    assert list(res.keys()) == [2, 3]
