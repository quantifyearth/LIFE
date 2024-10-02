import argparse
import os
from functools import partial
from multiprocessing import Pool
from typing import Optional, Tuple

# import pyshark # pylint: disable=W0611
import geopandas as gpd
import pyproj
import psycopg2
from postgis.psycopg import register
from shapely import from_wkb

from cleaning import tidy_data

SEASON_NAME = {
    1: "RESIDENT",
    2: "BREEDING",
    3: "NONBREEDING",
}

MAIN_STATEMENT = """
WITH habitat_seasons AS (
	SELECT
        assessment_habitats.assessment_id,
        assessment_habitats.habitat_id,
        CASE
            WHEN (assessment_habitats.supplementary_fields->>'season') ILIKE 'Resident' THEN 1
            WHEN (assessment_habitats.supplementary_fields->>'season') ILIKE 'Breeding%%' THEN 2
            WHEN (assessment_habitats.supplementary_fields->>'season') ILIKE 'Non%%Bree%%' THEN 3
            WHEN (assessment_habitats.supplementary_fields->>'season') ILIKE 'Pass%%' THEN 4
            WHEN (assessment_habitats.supplementary_fields->>'season') ILIKE '%%un%%n%%' THEN 1 -- capture 'uncertain' and 'unknown' as resident
            ELSE 1
        END AS seasonal
    FROM
        public.assessments
        LEFT JOIN taxons ON taxons.id = assessments.taxon_id
        LEFT JOIN assessment_habitats ON assessment_habitats.assessment_id = assessments.id
    WHERE
        assessments.latest = true
        AND (
            -- LIFE ignores marginal suitability
            assessment_habitats.supplementary_fields->>'suitability' IS NULL
            OR assessment_habitats.supplementary_fields->>'suitability' IN ('Suitable', 'Unknown')
        )
),
unique_seasons AS (
  	SELECT DISTINCT ON (taxons.scientific_name, habitat_seasons.seasonal)
        assessments.sis_taxon_id as id_no,
        assessment_ranges.seasonal,
        assessment_ranges.presence,
        assessment_ranges.origin,
        STRING_AGG(habitat_lookup.code, '|') OVER (PARTITION BY taxons.scientific_name, habitat_seasons.seasonal ORDER BY assessment_ranges.id) AS full_habitat_code,
        STRING_AGG(system_lookup.description->>'en', '|') OVER (PARTITION BY taxons.scientific_name, habitat_seasons.seasonal ORDER BY assessment_ranges.id) AS systems,
        STRING_AGG(assessment_ranges.id::text, '|') OVER (PARTITION BY taxons.scientific_name, habitat_seasons.seasonal ORDER BY assessment_ranges.id) AS ranges,
        (assessment_supplementary_infos.supplementary_fields->>'ElevationLower.limit')::numeric AS elevation_lower,
        (assessment_supplementary_infos.supplementary_fields->>'ElevationUpper.limit')::numeric AS elevation_upper,
        ROW_NUMBER() OVER (PARTITION BY taxons.scientific_name, habitat_seasons.seasonal ORDER BY assessments.id, assessment_ranges.id) AS rn
    FROM
        assessments
        LEFT JOIN taxons ON taxons.id = assessments.taxon_id
        LEFT JOIN assessment_ranges ON assessment_ranges.assessment_id = assessments.id
        LEFT JOIN habitat_seasons ON habitat_seasons.assessment_id = assessments.id AND habitat_seasons.seasonal = assessment_ranges.seasonal
        LEFT JOIN assessment_systems ON assessment_systems.assessment_id = assessments.id
        LEFT JOIN system_lookup ON assessment_systems.system_lookup_id = system_lookup.id
        LEFT JOIN habitat_lookup ON habitat_lookup.id = habitat_seasons.habitat_id
        LEFT JOIN assessment_supplementary_infos ON assessment_supplementary_infos.assessment_id = assessments.id
        LEFT JOIN red_list_category_lookup ON red_list_category_lookup.id = assessments.red_list_category_id
    WHERE
        assessments.latest = true
        AND taxons.class_id = 22672813 -- AVES
        AND habitat_seasons.habitat_id is not null
        AND assessment_ranges.presence IN %s
        AND assessment_ranges.origin IN (1, 2, 6)
        AND assessment_ranges.seasonal IN (1, 2, 3)
        AND red_list_category_lookup.code != 'EX'
    )
SELECT
    id_no,
    seasonal,
    elevation_lower,
    elevation_upper,
    full_habitat_code,
    ranges
FROM
    unique_seasons
WHERE
    rn = 1
    -- the below queries must happen on the aggregate data
    AND full_habitat_code NOT LIKE '7%%'
    AND full_habitat_code NOT LIKE '%%|7%%'
    AND systems NOT LIKE '%%Marine%%'
"""

GEOMETRY_STATEMENT = """
SELECT
    ST_UNION(assessment_ranges.geom::geometry) AS geometry
FROM
    assessment_ranges
WHERE
    assessment_ranges.id IN %s
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

def process_row(
    output_directory_path: str,
    presence: Tuple[int],
    row: Tuple,
) -> None:
    # The geometry is in CRS 4326, but the AoH work is done in World_Behrmann, aka Projected CRS: ESRI:54017
    src_crs = pyproj.CRS.from_epsg(4326)
    target_crs = src_crs #pyproj.CRS.from_string(target_projection)

    connection = psycopg2.connect(DB_CONFIG)
    register(connection)
    curs = connection.cursor()

    id_no, seasonal, elevation_lower, elevation_upper, full_habitat_code, range_ids = row

    cleaned_range_ids = set([int(x) for x in range_ids.split('|')])

    curs.execute(GEOMETRY_STATEMENT, (tuple(cleaned_range_ids), presence))
    geometry = curs.fetchall()
    if len(geometry) == 0:
        return
    elif len(geometry) > 1:
        raise ValueError("Expected just a single geometry value")

    x = (geometry[0][0])
    x = from_wkb(x.to_ewkb())

    gdf = gpd.GeoDataFrame(
        [[id_no, seasonal, int(elevation_lower) if elevation_lower else None, int(elevation_upper) if elevation_upper else None, full_habitat_code]],
        columns=["id_no", "seasonal", "elevation_lower", "elevation_upper", "full_habitat_code"],
        crs='epsg:4326', geometry=[x])
    graw = gdf.loc[0].copy()

    grow = tidy_data(graw)
    output_path = os.path.join(output_directory_path, f"{grow.id_no}_{SEASON_NAME[grow.seasonal]}.geojson")
    res = gpd.GeoDataFrame(grow.to_frame().transpose(), crs=src_crs, geometry="geometry")
    res_projected = res.to_crs(target_crs)
    res_projected.to_file(output_path, driver="GeoJSON")

def extract_data_per_species(
    output_directory_path: str,
    target_projection: Optional[str],
) -> None:

    connection = psycopg2.connect(DB_CONFIG)
    curs = connection.cursor()

    # engine = create_engine(DB_CONFIG, echo=False)
    for era, presence in [("current", (1, 2)), ("historic", (1, 2, 4, 5))]:
        era_output_directory_path = os.path.join(output_directory_path, era)
        os.makedirs(os.path.join(era_output_directory_path, era), exist_ok=True)

        curs.execute(MAIN_STATEMENT, (presence,))
        # This can be quite big (tens of thousands), but in modern computer term is quite small
        # and I need to make a follow on DB query per result.
        results = curs.fetchall()

        # The limiting amount here is how many concurrent connections the database can take
        with Pool(processes=20) as pool:
            pool.map(partial(process_row, era_output_directory_path, presence), results)

def main() -> None:
    parser = argparse.ArgumentParser(description="Process agregate species data to per-species-file.")
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
        args.output_directory_path,
        args.target_projection
    )

if __name__ == "__main__":
    main()
