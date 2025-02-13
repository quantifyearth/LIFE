import importlib
import logging
import os
from typing import Dict, List, Set, Tuple

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

def process_systems(
    systems_data: List[Tuple]
) -> None:
    if len(systems_data) == 0:
        raise ValueError("No systems found")
    if len(systems_data) > 1:
        raise ValueError("More than one systems aggregation found")

    systems = systems_data[0][0]
    if systems is None:
        raise ValueError("no systems info")
    if "Marine" in systems:
        raise ValueError("Marine in systems")

def process_habitats(
    habitats_data: List[Tuple],
) -> Dict:

    if len(habitats_data) == 0:
        raise ValueError("No habitats found")

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

    major_habitats_lvl_1 = {k: {int(v) for v in x} for k, x in major_habitats.items()}

    for _, season_major_habitats in major_habitats_lvl_1.items():
        if 7 in season_major_habitats:
            raise ValueError("Habitat 7 in major importance habitat list")
    for _, season_major_habitats in major_habitats.items():
        if not season_major_habitats - set([5.1, 5.5, 5.6, 5.14, 5.16]):
            raise ValueError("Freshwater lakes are major habitat")

    return habitats


def process_geometries(geometries_data: List[Tuple[int,shapely.Geometry]]) -> Dict[int,shapely.Geometry]:
    if len(geometries_data) == 0:
        raise ValueError("No geometries")

    geometries = {}
    for season, geometry in geometries_data:
        grange = shapely.normalize(shapely.from_wkb(geometry.to_ewkb()))

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

    return geometries


def tidy_reproject_save(
    gdf: gpd.GeoDataFrame,
    output_directory_path: str
) -> None:
    # The geometry is in CRS 4326, but the AoH work is done in World_Behrmann, aka Projected CRS: ESRI:54017
    src_crs = pyproj.CRS.from_epsg(4326)
    target_crs = src_crs #pyproj.CRS.from_string(target_projection)

    graw = gdf.loc[0].copy()
    grow = aoh_cleaning.tidy_data(graw)
    os.makedirs(output_directory_path, exist_ok=True)
    output_path = os.path.join(output_directory_path, f"{grow.id_no}_{grow.season}.geojson")
    res = gpd.GeoDataFrame(grow.to_frame().transpose(), crs=src_crs, geometry="geometry")
    res_projected = res.to_crs(target_crs)
    res_projected.to_file(output_path, driver="GeoJSON")

def process_and_save(
    row: Tuple,
    class_name: str,
    habitats,
    geometries,
    output_directory_path,
) -> None:

    id_no, assessment_id, elevation_lower, elevation_upper, scientific_name, family_name, threat_code = row

    seasons = set(geometries.keys()) | set(habitats.keys())

    if seasons == {1}:
        # Resident only
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
        tidy_reproject_save(gdf, output_directory_path)
    else:
        # Breeding and non-breeding
        # Sometimes in the IUCN database there's only data on one season (e.g., AVES 103838515), and so
        # we need to do another sanity check to make sure both have useful data before we write out

        geometries_seasons_breeding = set(geometries.keys())
        geometries_seasons_breeding.discard(3)
        geometries_breeding = [geometries[x] for x in geometries_seasons_breeding]
        if len(geometries_breeding) == 0:
            logger.debug("Dropping %s as no breeding geometries", id_no)
            return
        geometry_breeding = shapely.union_all(geometries_breeding)

        geometries_seasons_non_breeding = set(geometries.keys())
        geometries_seasons_non_breeding.discard(2)
        geometries_non_breeding = [geometries[x] for x in geometries_seasons_non_breeding]
        if len(geometries_non_breeding) == 0:
            logger.debug("Dropping %s as no non-breeding geometries", id_no)
            return
        geometry_non_breeding = shapely.union_all(geometries_non_breeding)

        habitats_seasons_breeding = set(habitats.keys())
        habitats_seasons_breeding.discard(3)
        habitats_breeding = set()
        for season in habitats_seasons_breeding:
            habitats_breeding |= habitats[season]
        if len(habitats_breeding) == 0:
            logger.debug("Dropping %s as no breeding habitats", id_no)
            return

        habitats_seasons_non_breeding = set(habitats.keys())
        habitats_seasons_non_breeding.discard(2)
        habitats_non_breeding = set()
        for season in habitats_seasons_non_breeding:
            habitats_non_breeding |= habitats[season]
        if len(habitats_non_breeding) == 0:
            logger.debug("Dropping %s as no non-breeding habitats", id_no)
            return

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
        tidy_reproject_save(gdf, output_directory_path)

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
        tidy_reproject_save(gdf, output_directory_path)
