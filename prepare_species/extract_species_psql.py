import argparse
import logging
import os
from functools import partial
from multiprocessing import Pool
from typing import Optional, Tuple

# import pyshark # pylint: disable=W0611
import geopandas as gpd
import psycopg2
import shapely
from postgis.psycopg import register

from common import process_geometries, process_habitats, process_systems, process_and_save

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)

MAIN_STATEMENT = """
SELECT
    assessments.sis_taxon_id as id_no,
    assessments.id as assessment_id,
    (assessment_supplementary_infos.supplementary_fields->>'ElevationLower.limit')::numeric AS elevation_lower,
    (assessment_supplementary_infos.supplementary_fields->>'ElevationUpper.limit')::numeric AS elevation_upper,
    taxons.scientific_name,
    taxons.family_name,
    red_list_category_lookup.code
FROM
    assessments
    LEFT JOIN assessment_scopes ON assessment_scopes.assessment_id = assessments.id
    LEFT JOIN taxons ON taxons.id = assessments.taxon_id
    LEFT JOIN assessment_supplementary_infos ON assessment_supplementary_infos.assessment_id = assessments.id
    LEFT JOIN red_list_category_lookup ON red_list_category_lookup.id = assessments.red_list_category_id
WHERE
    assessments.latest = true
    AND assessment_scopes.scope_lookup_id = 15 -- global assessments only
    AND taxons.class_name = %s
    AND taxons.infra_type is NULL -- no subspecies
    AND taxons.metadata->>'taxon_level' = 'Species'  -- no subpopulations
    AND red_list_category_lookup.code NOT IN ('EX')
"""

SYSTEMS_STATEMENT = """
SELECT
    STRING_AGG(system_lookup.description->>'en', '|') AS systems
FROM
    assessments
    LEFT JOIN assessment_systems ON assessment_systems.assessment_id = assessments.id
    LEFT JOIN system_lookup ON assessment_systems.system_lookup_id = system_lookup.id
WHERE
    assessments.id = %s
GROUP BY
    assessments.id
"""

HABITATS_STATEMENT = """
SELECT
    assessment_habitats.supplementary_fields->>'season',
    assessment_habitats.supplementary_fields->>'majorImportance',
    STRING_AGG(habitat_lookup.code, '|') AS full_habitat_code
FROM
    assessments
    LEFT JOIN assessment_habitats ON assessment_habitats.assessment_id = assessments.id
    LEFT JOIN habitat_lookup on habitat_lookup.id = assessment_habitats.habitat_id
WHERE
    assessments.id = %s
    AND (
        -- LIFE ignores marginal suitability, and ignores majorImportance
        assessment_habitats.supplementary_fields->>'suitability' IS NULL
        OR assessment_habitats.supplementary_fields->>'suitability' IN ('Suitable', 'Unknown')
    )
GROUP BY
    (assessment_habitats.supplementary_fields->>'season', assessment_habitats.supplementary_fields->>'majorImportance')
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

def process_row(
    class_name: str,
    output_directory_path: str,
    presence: Tuple[int],
    row: Tuple,
) -> None:

    connection = psycopg2.connect(DB_CONFIG)
    register(connection)
    cursor = connection.cursor()

    (id_no, assessment_id, _elevation_lower, _elevation_upper, _scientific_name, _family_name, _threat_code) = row

    cursor.execute(SYSTEMS_STATEMENT, (assessment_id,))
    systems_data = cursor.fetchall()
    try:
        process_systems(systems_data)
    except ValueError as exc:
        logger.info("Dropping %s: %s", id_no, str(exc))
        return

    cursor.execute(HABITATS_STATEMENT, (assessment_id,))
    habitats_data = cursor.fetchall()
    try:
        habitats = process_habitats(habitats_data)
    except ValueError as exc:
        logger.info("Dropping %s: %s", id_no, str(exc))
        return

    cursor.execute(GEOMETRY_STATEMENT, (assessment_id, presence))
    geometries_data = cursor.fetchall()
    try:
        geometries = process_geometries(geometries_data)
    except ValueError as exc:
        logger.info("Dropping %s: %s", id_no, str(exc))
        return

    process_and_save(
        row,
        class_name,
        habitats,
        geometries,
        output_directory_path
    )

def extract_data_per_species(
    class_name: str,
    output_directory_path: str,
    _target_projection: Optional[str],
) -> None:

    connection = psycopg2.connect(DB_CONFIG)
    cursor = connection.cursor()

    cursor.execute(MAIN_STATEMENT, (class_name,))
    # This can be quite big (tens of thousands), but in modern computer term is quite small
    # and I need to make a follow on DB query per result.
    results = cursor.fetchall()

    logger.info("Found %d species in class %s", len(results), class_name)

    for era, presence in [("current", (1, 2)), ("historic", (1, 2, 4, 5))]:
        era_output_directory_path = os.path.join(output_directory_path, era)

        # The limiting amount here is how many concurrent connections the database can take
        with Pool(processes=20) as pool:
            pool.map(partial(process_row, class_name, era_output_directory_path, presence), results)

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
