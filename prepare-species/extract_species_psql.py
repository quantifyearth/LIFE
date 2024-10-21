import argparse
import logging
import os
from functools import partial
from multiprocessing import Pool
from typing import Optional, Tuple

# import pyshark # pylint: disable=W0611
import geopandas as gpd
import pyproj
import psycopg2
import shapely
from postgis.psycopg import register

from cleaning import tidy_data

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)

SEASON_NAME = {
    1: "RESIDENT",
    2: "BREEDING",
    3: "NONBREEDING",
}

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
GROUP BY (assessment_habitats.supplementary_fields->>'season')
"""

GEOMETRY_STATEMENT = """
SELECT
    assessment_ranges.seasonal,
    ST_UNION(assessment_ranges.geom::geometry) OVER (PARTITION BY assessment_ranges.seasonal) AS geometry
FROM
    assessments
    LEFT JOIN assessment_ranges On assessment_ranges.assessment_id = assessments.id
WHERE
    assessments.id = %s
    AND assessment_ranges.presence IN %s
    AND assessment_ranges.origin IN (1, 2, 6)
    AND assessment_ranges.seasonal IN (1, 2, 3)
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
    grow = tidy_data(graw)
    os.makedirs(output_directory_path, exist_ok=True)
    output_path = os.path.join(output_directory_path, f"{grow.id_no}_{grow.season}.geojson")
    res = gpd.GeoDataFrame(grow.to_frame().transpose(), crs=src_crs, geometry="geometry")
    res_projected = res.to_crs(target_crs)
    res_projected.to_file(output_path, driver="GeoJSON")


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
    raw_habitats = cursor.fetchall()

    if len(raw_habitats) == 0:
        logger.debug("Dropping %s as no habitats found", id_no)
        return

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

    habitats = {}
    for season, habitat_values, systems in raw_habitats:

        if season in ['passage', 'Passage']:
            continue
        elif season in ['resident', 'Resident', 'Seasonal Occurrence Unknown', 'unknown', None]:
            season_code = 1
        elif season in ['breeding', 'Breeding Season']:
            season_code = 2
        elif season in ['non-breeding', 'Non-Breeding Season']:
            season_code = 3
        else:
            raise ValueError(f"Unexpected season {season} for {id_no}")

        if systems is None:
            logger.debug("Dropping %s: no systems in DB", id_no)
            continue
        if "Marine" in systems:
            logger.debug("Dropping %s: marine in systems", id_no)
            return

        if habitat_values is None:
            logger.debug("Dropping %s: no habitats in DB", id_no)
            continue
        habitat_set = set([x for x in habitat_values.split('|')])
        if len(habitat_set) == 0:
            logger.debug("Dropping %s: No habitats", id_no)
            continue
        if any([x.startswith('7') for x in habitat_set]):
            logger.debug("Dropping %s: Habitat 7 in habitat list", id_no)
            return

        try:
            habitats[season_code] |= habitat_set
        except KeyError:
            habitats[season_code] = habitat_set

    if len(habitats) == 0:
        return

    cursor.execute(GEOMETRY_STATEMENT, (assessment_id, presence))
    geometries_data = cursor.fetchall()
    if len(geometries_data) == 0:
        logger.info("Dropping %s: no habitats", id_no)
        return
    geometries = {}
    for season, geometry in geometries_data:
        geometries[season] = shapely.normalize(shapely.from_wkb(geometry.to_ewkb()))

    seasons = set(geometries.keys()) | set(habitats.keys())

    if seasons == {1}:
        # Resident only
        gdf = gpd.GeoDataFrame(
            [[id_no, SEASON_NAME[1], int(elevation_lower) if elevation_lower else None, int(elevation_upper) if elevation_upper else None, '|'.join(list(habitats[1])), geometries[1]]],
            columns=["id_no", "season", "elevation_lower", "elevation_upper", "full_habitat_code", "geometry"],
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
            [[id_no, SEASON_NAME[2], int(elevation_lower) if elevation_lower else None, int(elevation_upper) if elevation_upper else None, '|'.join(list(habitats_breeding)), geometry_breeding]],
            columns=["id_no", "season", "elevation_lower", "elevation_upper", "full_habitat_code", "geometry"],
            crs='epsg:4326'
        )
        tidy_reproject_save(gdf, output_directory_path)

        gdf = gpd.GeoDataFrame(
            [[id_no, SEASON_NAME[3], int(elevation_lower) if elevation_lower else None, int(elevation_upper) if elevation_upper else None, '|'.join(list(habitats_non_breeding)), geometry_non_breeding]],
            columns=["id_no", "season", "elevation_lower", "elevation_upper", "full_habitat_code", "geometry"],
            crs='epsg:4326',
        )
        tidy_reproject_save(gdf, output_directory_path)



def extract_data_per_species(
    classname: str,
    output_directory_path: str,
    target_projection: Optional[str],
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
