import argparse
import importlib
import logging
import os
from functools import partial
from multiprocessing import Pool
from typing import Dict, List, Optional, Set, Tuple

# import pyshark # pylint: disable=W0611
import geopandas as gpd
import pyproj
import psycopg2
import shapely
from postgis.psycopg import register

aoh_cleaning = importlib.import_module("aoh-calculator.cleaning")

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)

SEASON_NAME = {
    1: "RESIDENT",
    2: "BREEDING",
    3: "NONBREEDING",
}

COLUMNS = [
    "id_no",
    "season",
    "elevation_lower",
    "elevation_upper",
    "full_habitat_code",
    "geometry"
]

MAIN_STATEMENT = """
SELECT
    assessments.sis_taxon_id as id_no,
    assessments.id as assessment_id,
    (assessment_supplementary_infos.supplementary_fields->>'ElevationLower.limit')::numeric AS elevation_lower,
    (assessment_supplementary_infos.supplementary_fields->>'ElevationUpper.limit')::numeric AS elevation_upper
FROM
    assessments
    LEFT JOIN taxons ON taxons.id = assessments.taxon_id
    LEFT JOIN assessment_supplementary_infos ON assessment_supplementary_infos.assessment_id = assessments.id
    LEFT JOIN red_list_category_lookup ON red_list_category_lookup.id = assessments.red_list_category_id
WHERE
    assessments.latest = true
    AND taxons.class_name = %s
    AND red_list_category_lookup.code NOT IN ('EX')
"""

HABITATS_STATEMENT = """
SELECT
    assessment_habitats.supplementary_fields->>'season',
    assessment_habitats.supplementary_fields->>'majorImportance',
    STRING_AGG(habitat_lookup.code, '|') AS full_habitat_code,
    STRING_AGG(system_lookup.description->>'en', '|') AS systems
FROM
    assessments
    LEFT JOIN assessment_habitats ON assessment_habitats.assessment_id = assessments.id
    LEFT JOIN habitat_lookup on habitat_lookup.id = assessment_habitats.habitat_id
    LEFT JOIN assessment_systems ON assessment_systems.assessment_id = assessments.id
    LEFT JOIN system_lookup ON assessment_systems.system_lookup_id = system_lookup.id
WHERE
    assessments.id = %s
    AND (
        -- LIFE ignores marginal suitability, and ignores majorImportance
        assessment_habitats.supplementary_fields->>'suitability' IS NULL
        OR assessment_habitats.supplementary_fields->>'suitability' IN ('Suitable', 'Unknown')
    )
GROUP BY (assessment_habitats.supplementary_fields->>'season', assessment_habitats.supplementary_fields->>'majorImportance')
"""

GEOMETRY_STATEMENT = """
SELECT
    assessment_ranges.seasonal,
    ST_UNION(assessment_ranges.geom::geometry) OVER (PARTITION BY assessment_ranges.seasonal) AS geometry
FROM
    assessments
    LEFT JOIN assessment_ranges On assessment_ranges.assessment_id = assessments.id
WHERE
    -- LIFE doesn't use passage (season 4), and treats unknown (season 5) as resident.
    assessments.id = %s
    AND assessment_ranges.presence IN %s
    AND assessment_ranges.origin IN (1, 2, 6)
    AND assessment_ranges.seasonal IN (1, 2, 3, 5)
"""

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_CONFIG = (
	f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

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

def process_habitats(
    habitats_data: List,
) -> Dict:

    if len(habitats_data) == 0:
        raise ValueError("No habitats found")

    # Clean up habitats to ensure they're unique (the system agg in the SQL statement might duplicate them)
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
    major_habitats_lvl_1 : Dict[Set[int]] = {}
    for season, major_importance, habitat_values, systems in habitats_data:

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

        if systems is None:
            continue
        if "Marine" in systems:
            raise ValueError("Marine in systems")

        if habitat_values is None:
            continue
        habitat_set = set(habitat_values.split('|'))
        if len(habitat_set) == 0:
            continue

        habitats[season_code] = habitat_set | habitats.get(season_code, set())

        if major_importance == 'Yes':
            major_habitats_lvl_1[season_code] = \
                {int(float(x)) for x in habitat_set} | major_habitats_lvl_1.get(season_code, set())

    # habitat based filtering
    if len(habitats) == 0:
        raise ValueError("No filtered habitats")

    for _, major_habitats in major_habitats_lvl_1.items():
        if any((x == 7) for x in major_habitats):
            raise ValueError("Habitat 7 in major importance habitat list")

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

def process_row(
    output_directory_path: str,
    presence: Tuple[int],
    row: Tuple,
) -> None:

    connection = psycopg2.connect(DB_CONFIG)
    register(connection)
    cursor = connection.cursor()

    id_no, assessment_id, elevation_lower, elevation_upper = row

    cursor.execute(HABITATS_STATEMENT, (assessment_id,))
    habitats_data = cursor.fetchall()
    try:
        habitats = process_habitats(habitats_data)
    except ValueError as exc:
        logging.info("Dropping %s: %s", id_no, str(exc))
        return

    cursor.execute(GEOMETRY_STATEMENT, (assessment_id, presence))
    geometries_data = cursor.fetchall()
    try:
        geometries = process_geometries(geometries_data)
    except ValueError as exc:
        logging.info("Dropping %s: %s", id_no, str(exc))
        return

    seasons = set(geometries.keys()) | set(habitats.keys())

    if seasons == {1}:
        # Resident only
        gdf = gpd.GeoDataFrame(
            [[
                id_no,
                SEASON_NAME[1],
                int(elevation_lower) if elevation_lower else None, int(elevation_upper) if elevation_upper else None,
                '|'.join(list(habitats[1])),
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
                SEASON_NAME[2],
                int(elevation_lower) if elevation_lower else None, int(elevation_upper) if elevation_upper else None,
                '|'.join(list(habitats_breeding)),
                geometry_breeding
            ]],
            columns=COLUMNS,
            crs='epsg:4326'
        )
        tidy_reproject_save(gdf, output_directory_path)

        gdf = gpd.GeoDataFrame(
            [[
                id_no, SEASON_NAME[3],
                int(elevation_lower) if elevation_lower else None, int(elevation_upper) if elevation_upper else None,
                '|'.join(list(habitats_non_breeding)),
                geometry_non_breeding
            ]],
            columns=COLUMNS,
            crs='epsg:4326',
        )
        tidy_reproject_save(gdf, output_directory_path)


def extract_data_per_species(
    classname: str,
    output_directory_path: str,
    _target_projection: Optional[str],
) -> None:

    connection = psycopg2.connect(DB_CONFIG)
    cursor = connection.cursor()

    for era, presence in [("current", (1, 2)), ("historic", (1, 2, 4, 5))]:
        era_output_directory_path = os.path.join(output_directory_path, era)

        cursor.execute(MAIN_STATEMENT, (classname,))
        # This can be quite big (tens of thousands), but in modern computer term is quite small
        # and I need to make a follow on DB query per result.
        results = cursor.fetchall()

        logger.info("Found %d species in class %s in scenarion %s", len(results), classname, era)

        # The limiting amount here is how many concurrent connections the database can take
        with Pool(processes=20) as pool:
            pool.map(partial(process_row, era_output_directory_path, presence), results)

def main() -> None:
    parser = argparse.ArgumentParser(description="Process agregate species data to per-species-file.")
    parser.add_argument(
        '--class',
        type=str,
        help="Species class name",
        required=True,
        dest="classname",
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Directory where per species Geojson is stored',
        required=True,
        dest='output_directory_path',
    )
    parser.add_argument(
        '--projection',
        type=str,
        help="Target projection",
        required=False,
        dest="target_projection",
        default="ESRI:54017"
    )
    args = parser.parse_args()

    extract_data_per_species(
        args.classname,
        args.output_directory_path,
        args.target_projection
    )

if __name__ == "__main__":
    main()
