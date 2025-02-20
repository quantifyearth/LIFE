import importlib
import logging
import os
from typing import Any, Dict, List, Set, Tuple

import geopandas as gpd
import pyproj
import shapely

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)

aoh_cleaning = importlib.import_module("aoh-calculator.cleaning")

SEASON_NAME = {
    1: "RESIDENT",
    2: "BREEDING",
    3: "NONBREEDING",
}

COLUMNS = [
    "id_no",
    "assessment_id",
    "season",
    "elevation_lower",
    "elevation_upper",
    "full_habitat_code",
    "scientific_name",
    "family_name",
    "class_name",
    "category",
    "geometry",
]


class SpeciesReport:

    REPORT_COLUMNS = [
        "id_no",
        "assessment_id",
        "scientific_name",
        "has_systems",
        "not_marine",
        "has_habitats",
        "keeps_habitats",
        "not_major_caves",
        "not_major_freshwater_lakes",
        "has_geometries",
        "keeps_geometries",
        "is_resident",
        "is_migratory",
        "has_breeding_geometry",
        "has_nonbreeding_geometry",
        "has_breeding_habitat",
        "has_nonbreeding_habitat",
        "filename",
    ]

    def __init__(self, id_no, assessment_id, scientific_name):
        self.info = {k: False for k in self.REPORT_COLUMNS}
        self.id_no = id_no
        self.assessment_id = assessment_id
        self.scientific_name = scientific_name

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.REPORT_COLUMNS:
            self.info[name] = value
        super().__setattr__(name, value)

    def __getattr__(self, name: str) -> Any:
        if name in self.REPORT_COLUMNS:
            return self.info[name]
        return None

    def as_row(self) -> List:
        return [self.info[k] for k in self.REPORT_COLUMNS]


def process_systems(
    systems_data: List[Tuple],
    report: SpeciesReport,
) -> None:
    if len(systems_data) == 0:
        raise ValueError("No systems found")
    if len(systems_data) > 1:
        raise ValueError("More than one systems aggregation found")
    systems = systems_data[0][0]
    if systems is None:
        raise ValueError("no systems info")
    report.has_systems = True

    if "Marine" in systems:
        raise ValueError("Marine in systems")
    report.not_marine = True

def process_habitats(
    habitats_data: List[Tuple],
    report: SpeciesReport,
) -> Dict:

    if len(habitats_data) == 0:
        raise ValueError("No habitats found")
    report.has_habitats = True

    # Clean up habitats to ensure they're unique
    # In the database there are the following seasons:
    #    breeding
    #    Breeding Season
    #    non-breeding
    #    Non-Breeding Season
    #    passage
    #    Passage
    #    resident
    #    Resident
    #    Seasonal Occurrence Unknown
    #    unknown
    #    null

    habitats : Dict[Set[str]] = {}
    major_habitats : Dict[Set[int]] = {}
    for season, major_importance, habitat_values in habitats_data:

        match season:
            case 'passage' | 'Passage':
                continue
            case 'resident' | 'Resident' | 'Seasonal Occurrence Unknown' | 'unknown' | None:
                season_code = 1
            case 'breeding' | 'Breeding Season':
                season_code = 2
            case 'non-breeding' | 'Non-Breeding Season':
                season_code = 3
            case _:
                raise ValueError(f"Unexpected season {season}")

        if habitat_values is None:
            continue
        habitat_set = set(habitat_values.split('|'))
        if len(habitat_set) == 0:
            continue

        habitats[season_code] = habitat_set | habitats.get(season_code, set())

        if major_importance == 'Yes':
            major_habitats[season_code] = \
                {float(x) for x in habitat_set} | major_habitats.get(season_code, set())

    # habitat based filtering
    if len(habitats) == 0:
        raise ValueError("No filtered habitats")
    report.keeps_habitats = True

    major_habitats_lvl_1 = {k: {int(v) for v in x} for k, x in major_habitats.items()}

    for _, season_major_habitats in major_habitats_lvl_1.items():
        if 7 in season_major_habitats:
            raise ValueError("Habitat 7 in major importance habitat list")
    report.not_major_caves = True
    for _, season_major_habitats in major_habitats.items():
        if not season_major_habitats - set([5.1, 5.5, 5.6, 5.14, 5.16]):
            raise ValueError("Freshwater lakes are major habitat")
    report.not_major_freshwater_lakes = True

    return habitats


def process_geometries(
    geometries_data: List[Tuple[int,shapely.Geometry]],
    report: SpeciesReport,
) -> Dict[int,shapely.Geometry]:
    if len(geometries_data) == 0:
        raise ValueError("No geometries")
    report.has_geometries = True

    geometries = {}
    for season, geometry in geometries_data:
        grange = shapely.normalize(shapely.from_wkb(geometry))

        match season:
            case 1 | 5:
                season_code = 1
            case 2 | 3:
                season_code = season
            case _:
                raise ValueError(f"Unexpected season: {season}")

        try:
            geometries[season_code] = shapely.union(geometries[season_code], grange)
        except KeyError:
            geometries[season_code] = grange

    if len(geometries) == 0:
        raise ValueError("No filtered geometries")
    report.keeps_geometries = True

    return geometries

def tidy_reproject_save(
    gdf: gpd.GeoDataFrame,
    report: SpeciesReport,
    output_directory_path: str
) -> None:
    # The geometry is in CRS 4326, but the AoH work is done in World_Behrmann, aka Projected CRS: ESRI:54017
    src_crs = pyproj.CRS.from_epsg(4326)
    target_crs = src_crs #pyproj.CRS.from_string(target_projection)

    graw = gdf.loc[0].copy()
    grow = aoh_cleaning.tidy_data(graw)
    output_path = os.path.join(output_directory_path, f"{grow.id_no}_{grow.season}.geojson")
    res = gpd.GeoDataFrame(grow.to_frame().transpose(), crs=src_crs, geometry="geometry")
    res_projected = res.to_crs(target_crs)
    report.filename = output_path
    res_projected.to_file(output_path, driver="GeoJSON")

def process_and_save(
    row: Tuple,
    report: SpeciesReport,
    class_name: str,
    habitats,
    geometries,
    output_directory_path: str,
) -> None:

    id_no, assessment_id, elevation_lower, elevation_upper, scientific_name, family_name, threat_code = row

    seasons = set(geometries.keys()) | set(habitats.keys())

    if seasons == {1}:
        # Resident only
        report.is_resident = True
        gdf = gpd.GeoDataFrame(
            [[
                id_no,
                assessment_id,
                SEASON_NAME[1],
                int(elevation_lower) if elevation_lower is not None else None,
                int(elevation_upper) if elevation_upper is not None else None,
                '|'.join(list(habitats[1])),
                scientific_name,
                family_name,
                class_name,
                threat_code,
                geometries[1]
            ]],
            columns=COLUMNS,
            crs='epsg:4326'
        )
        tidy_reproject_save(gdf, report, output_directory_path)
    else:
        # Breeding and non-breeding
        # Sometimes in the IUCN database there's only data on one season (e.g., AVES 103838515), and so
        # we need to do another sanity check to make sure both have useful data before we write out
        report.is_migratory = True

        geometries_seasons_breeding = set(geometries.keys())
        geometries_seasons_breeding.discard(3)
        geometries_breeding = [geometries[x] for x in geometries_seasons_breeding]
        if len(geometries_breeding) == 0:
            logger.debug("Dropping %s as no breeding geometries", id_no)
            return
        report.has_breeding_geoemetry = True
        geometry_breeding = shapely.union_all(geometries_breeding)

        geometries_seasons_non_breeding = set(geometries.keys())
        geometries_seasons_non_breeding.discard(2)
        geometries_non_breeding = [geometries[x] for x in geometries_seasons_non_breeding]
        if len(geometries_non_breeding) == 0:
            logger.debug("Dropping %s as no non-breeding geometries", id_no)
            return
        report.has_nonbreeding_geoemetry = True
        geometry_non_breeding = shapely.union_all(geometries_non_breeding)

        habitats_seasons_breeding = set(habitats.keys())
        habitats_seasons_breeding.discard(3)
        habitats_breeding = set()
        for season in habitats_seasons_breeding:
            habitats_breeding |= habitats[season]
        if len(habitats_breeding) == 0:
            logger.debug("Dropping %s as no breeding habitats", id_no)
            return
        report.has_breeding_habitats = True

        habitats_seasons_non_breeding = set(habitats.keys())
        habitats_seasons_non_breeding.discard(2)
        habitats_non_breeding = set()
        for season in habitats_seasons_non_breeding:
            habitats_non_breeding |= habitats[season]
        if len(habitats_non_breeding) == 0:
            logger.debug("Dropping %s as no non-breeding habitats", id_no)
            return
        report.has_nonbreeding_habitats = True

        gdf = gpd.GeoDataFrame(
            [[
                id_no,
                assessment_id,
                SEASON_NAME[2],
                int(elevation_lower) if elevation_lower is not None else None,
                int(elevation_upper) if elevation_upper is not None else None,
                '|'.join(list(habitats_breeding)),
                scientific_name,
                family_name,
                class_name,
                threat_code,
                geometry_breeding
            ]],
            columns=COLUMNS,
            crs='epsg:4326'
        )
        tidy_reproject_save(gdf, report, output_directory_path)

        gdf = gpd.GeoDataFrame(
            [[
                id_no,
                assessment_id,
                SEASON_NAME[3],
                int(elevation_lower) if elevation_lower is not None else None,
                int(elevation_upper) if elevation_upper is not None else None,
                '|'.join(list(habitats_non_breeding)),
                scientific_name,
                family_name,
                class_name,
                threat_code,
                geometry_non_breeding
            ]],
            columns=COLUMNS,
            crs='epsg:4326',
        )
        tidy_reproject_save(gdf, report, output_directory_path)
