import pytest

from prepare_species.common import process_habitats, process_geometries, process_systems, SpeciesReport

def test_empty_report() -> None:
    report = SpeciesReport(1, 2, "name")

    assert report.id_no == 1
    assert report.assessment_id == 2
    assert report.scientific_name == "name"
    assert not report.overriden
    assert not report.has_systems
    assert not report.not_marine
    assert not report.has_habitats
    assert not report.keeps_habitats
    assert not report.not_major_caves
    assert not report.not_major_freshwater_lakes
    assert not report.has_geometries
    assert not report.keeps_geometries
    assert not report.is_resident
    assert not report.is_migratory
    assert not report.has_breeding_geometry
    assert not report.has_nonbreeding_geometry
    assert not report.has_breeding_habitat
    assert not report.has_nonbreeding_habitat
    assert not report.filename

    row = report.as_row()
    assert row[:3] == [1, 2, "name"]
    assert not all(row[3:])

@pytest.mark.parametrize("label", [
    "resident",
    "Resident",
    "unknown",
    "Seasonal Occurrence Unknown",
    None
])
def test_simple_resident_species_habitat_filter(label):
    habitat_data = [
        (label, "Yes", "4.1|4.2"),
        (label, "No", "4.3"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_habitats(habitat_data, report)

    assert list(res.keys()) == [1]
    assert res[1] == set(["4.1", "4.2", "4.3"])
    assert report.has_habitats
    assert report.keeps_habitats
    assert report.not_major_caves
    assert report.not_major_freshwater_lakes

@pytest.mark.parametrize("breeding_label,non_breeding_label", [
    ("breeding", "non-breeding"),
    ("Breeding Season", "non-breeding"),
    ("breeding", "Non-Breeding Season"),
    ("Breeding Season", "Non-Breeding Season"),
])
def test_simple_migratory_species_habitat_filter(breeding_label, non_breeding_label):
    habitat_data = [
        (breeding_label, "Yes", "4.1|4.2"),
        (non_breeding_label, "No", "4.3"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_habitats(habitat_data, report)

    assert list(res.keys()) == [2, 3]
    assert res[2] == set(["4.1", "4.2"])
    assert res[3] == set(["4.3"])
    assert report.has_habitats
    assert report.keeps_habitats
    assert report.not_major_caves
    assert report.not_major_freshwater_lakes

def test_reject_if_caves_in_major_habitat():
    habitat_data = [
        ("resident", "Yes", "4.1|7.2"),
        ("resident", "No", "4.3"),
    ]
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data, report)
    assert report.has_habitats
    assert report.keeps_habitats
    assert not report.not_major_caves

def test_do_not_reject_if_caves_in_minor_habitat():
    habitat_data = [
        ("resident", "Yes", "4.1|4.2"),
        ("resident", "No", "7.3"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_habitats(habitat_data, report)

    # Just resident
    assert list(res.keys()) == [1]
    assert res[1] == set(["4.1", "4.2", "7.3"])
    assert report.has_habitats
    assert report.keeps_habitats
    assert report.not_major_caves
    assert report.not_major_freshwater_lakes

@pytest.mark.parametrize("label", [
    "passage",
    "Passage",
])
def test_passage_habitat_ignored(label):
    habitat_data = [
        ("resident", "Yes", "4.1|4.2"),
        (label, "No", "4.3"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_habitats(habitat_data, report)

    # Just resident
    assert list(res.keys()) == [1]
    assert res[1] == set(["4.1", "4.2"])
    assert report.has_habitats
    assert report.keeps_habitats
    assert report.not_major_caves
    assert report.not_major_freshwater_lakes

def test_fail_no_habitats_before_filter():
    habitat_data = []
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data, report)
    assert not report.has_habitats

def test_fail_no_habitats_after_filter():
    habitat_data = [
        ("passage", "Yes", "4.1|7.2"),
    ]
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data, report)
    assert report.has_habitats
    assert not report.keeps_habitats

def test_fail_if_unrecognised_season_for_habitat():
    habitat_data = [
        ("resident", "Yes", "4.1|4.2"),
        ("zarquon", "Yes", "4.3"),
    ]
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data, report)
    assert report.has_habitats
    assert not report.keeps_habitats

def test_empty_geometry_list():
    geoemetries_data = []
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_geometries(geoemetries_data, report)
    assert not report.has_habitats
    assert not report.keeps_habitats

@pytest.mark.parametrize("label", [
    1,
    5
])
def test_simple_resident_species_geometry_filter(label):
    habitat_data = [
        (label, "000000000140000000000000004010000000000000"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_geometries(habitat_data, report)

    assert list(res.keys()) == [1]
    assert report.has_geometries
    assert report.keeps_geometries

def test_simple_migratory_species_geometry_filter():
    habitat_data = [
        (2, "000000000140000000000000004010000000000000"),
        (3, "000000000140000000000000004010000000000000"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_geometries(habitat_data, report)

    assert list(res.keys()) == [2, 3]
    assert report.has_geometries
    assert report.keeps_geometries

def test_13394_habitat_filter():
    habitat_data = [
        ("resident", "Yes", "5.1"),
        ("resident", "No", "1.6|1.9"),
    ]
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        _ = process_habitats(habitat_data, report)
    assert report.has_habitats
    assert report.keeps_habitats
    assert not report.not_major_freshwater_lakes

def test_similar_13394_habitat_filter():
    habitat_data = [
        ("resident", "Yes", "5.1|1.7"),
        ("resident", "No", "1.6|1.9"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_habitats(habitat_data, report)

    # Just resident
    assert list(res.keys()) == [1]
    assert res[1] == set(["5.1", "1.7", "1.6", "1.9"])
    assert report.has_habitats
    assert report.keeps_habitats
    assert report.not_major_caves
    assert report.not_major_freshwater_lakes

def test_inverted_13394_habitat_filter():
    habitat_data = [
        ("resident", "No", "5.1"),
        ("resident", "Yes", "1.6|1.9"),
    ]
    report = SpeciesReport(1, 2, "name")
    res = process_habitats(habitat_data, report)

    # Just resident
    assert list(res.keys()) == [1]
    assert res[1] == set(["5.1", "1.6", "1.9"])
    assert report.has_habitats
    assert report.keeps_habitats
    assert report.not_major_caves
    assert report.not_major_freshwater_lakes

def test_reject_if_marine_in_system():
    systems_data = [
        ("Terrestrial|Marine",)
    ]
    report = SpeciesReport(1, 2, "name")
    with pytest.raises(ValueError):
        process_systems(systems_data, report)
    assert report.has_systems
    assert not report.not_marine

def test_reject_if_marine_in_system_with_override():
    systems_data = [
        ("Terrestrial|Marine",)
    ]
    report = SpeciesReport(1, 2, "name")
    report.overriden = True
    process_systems(systems_data, report)
    assert report.has_systems
    assert not report.not_marine

def test_pass_if_marine_not_in_system():
    systems_data = [
        ("Terrestrial",)
    ]
    report = SpeciesReport(1, 2, "name")
    process_systems(systems_data, report)
    assert report.has_systems
    assert report.not_marine
