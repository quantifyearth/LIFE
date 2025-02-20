import argparse
import logging
import os
from functools import partial
from multiprocessing import Pool
from typing import Optional, Tuple

import duckdb
import pandas as pd

from common import process_geometries, process_habitats, process_systems, process_and_save, SpeciesReport

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.INFO)

MAIN_STATEMENT = """
SELECT
    assessments.internalTaxonId AS id_no,
    assessments.assessmentId as assessment_id,
    assessment_supplementary_infos."ElevationLower.limit" AS elevation_lower,
    assessment_supplementary_infos."ElevationUpper.limit" AS elevation_upper,
    taxons.scientificName AS scientific_name,
    taxons.familyName AS family_name,
    assessments.redlistCategory AS code
FROM
    assessments
    LEFT JOIN taxons ON assessments.internalTaxonId = taxons.internalTaxonID
    LEFT JOIN assessment_supplementary_infos ON assessment_supplementary_infos.assessmentID = assessments.assessmentId
WHERE
    assessments.scopes = 'Global'
    AND taxons.className = ?
    AND assessments.redlistCategory NOT IN ('Extinct')
"""

SYSTEMS_STATEMENT = """
SELECT
    systems
FROM
    assessments
WHERE
    assessmentId = ?
"""

HABITATS_STATEMENT = """
SELECT
    season,
    majorImportance,
    STRING_AGG(code, '|') AS full_habitat_code
FROM
    habitats
WHERE
    assessmentId = ?
    -- LIFE ignores marginal suitability
    AND suitability IN ('Suitable', 'Unknown')
GROUP BY
    (season, majorImportance)
"""

# In the PostGIS version we do an ST_UNION to join all the geometries for a given
# season before returning them. But for DuckDB I found that for certain species
# (e.g., Leopards, taxon ID 15954) this was very slow, and so I instead let
# each row stand alone, and the existing code that squashes together resident with
# breeding/non-breeding as appropriate will happen to also squash duplicate seasons
GEOMETRY_STATEMENT = """
SELECT
    seasonal,
    ST_AsWKB(geometry::geometry) AS geometry
FROM
    ranges
WHERE
    ranges.id_no = ?
    AND presence IN ?
    AND origin IN (1, 2, 6)
    AND seasonal IN (1, 2, 3, 5)
"""

def process_row(
    class_name: str,
    output_directory_path: str,
    presence: Tuple[int],
    row: Tuple,
) -> SpeciesReport:

    (id_no, assessment_id, _elevation_lower, _elevation_upper, scientific_name, _family_name, _threat_code) = row
    logger.debug("Processing %s", id_no)

    report = SpeciesReport(id_no, assessment_id, scientific_name)

    con = duckdb.connect(":default:")

    logger.debug("systems lookup")
    systems_data = con.execute(SYSTEMS_STATEMENT, [assessment_id]).fetchall()
    try:
        process_systems(systems_data, report)
    except ValueError as exc:
        logger.debug("Dropping %s: %s", id_no, str(exc))
        return report

    logger.debug("habitats lookup")
    habitats_data = con.execute(HABITATS_STATEMENT, [assessment_id]).fetchall()
    try:
        habitats = process_habitats(habitats_data, report)
    except ValueError as exc:
        logger.debug("Dropping %s: %s", id_no, str(exc))
        return report

    logger.debug("geometries lookup")
    geometries_data = con.execute(GEOMETRY_STATEMENT, [id_no, presence]).fetchall()
    logger.debug("geometries processing")
    try:
        geometries = process_geometries(geometries_data, report)
    except ValueError as exc:
        logger.debug("Dropping %s: %s", id_no, str(exc))
        return report

    logger.debug("saving")
    process_and_save(
        row,
        report,
        class_name,
        habitats,
        geometries,
        output_directory_path
    )

    return report

def extract_data_per_species(
    class_name: str,
    batch_dir_path: str,
    ranges_shape_path: str,
    output_directory_path: str,
    _target_projection: Optional[str],
) -> None:
    os.makedirs(output_directory_path, exist_ok=True)

    con = duckdb.connect(":default:")

    taxons_df = pd.read_csv(os.path.join(batch_dir_path, "taxonomy.csv"))
    assessments_df = pd.read_csv(os.path.join(batch_dir_path, "assessments.csv"))
    all_other_fields_df = pd.read_csv(os.path.join(batch_dir_path, "all_other_fields.csv"))
    habitats_df = pd.read_csv(os.path.join(batch_dir_path, "habitats.csv"))

    duckdb.register("taxons", taxons_df)
    duckdb.register("assessments", assessments_df)
    duckdb.register("assessment_supplementary_infos", all_other_fields_df)
    duckdb.register("habitats", habitats_df)

    duckdb.install_extension("spatial")
    duckdb.load_extension("spatial")
    duckdb.query(f"create table ranges as select * from '{ranges_shape_path}'")

    for era, presence in [("current", (1, 2)), ("historic", (1, 2, 4, 5))]:
        era_output_directory_path = os.path.join(output_directory_path, era)
        os.makedirs(era_output_directory_path, exist_ok=True)

        con = duckdb.connect(":default:")
        results = con.execute(MAIN_STATEMENT, [class_name]).fetchall()

        with Pool(processes=100) as pool:
            reports = pool.map(partial(process_row, class_name, era_output_directory_path, presence), results)

        reports_df = pd.DataFrame(
            [x.as_row() for x in reports],
            columns=SpeciesReport.REPORT_COLUMNS
        ).sort_values('id_no')
        reports_df.to_csv(os.path.join(era_output_directory_path, "report.csv"), index=False)

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
        '--batchdir',
        type=str,
        help="IUCN species batch download directory",
        required=True,
        dest="batchdir",
    )
    parser.add_argument(
        '--ranges',
        type=str,
        help="related taxa ranges shapefle",
        required=True,
        dest="ranges",
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
        args.batchdir,
        args.ranges,
        args.output_directory_path,
        args.target_projection
    )

if __name__ == "__main__":
    main()
